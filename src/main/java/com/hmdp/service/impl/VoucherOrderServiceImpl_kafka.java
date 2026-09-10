package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillMessage;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillMessageMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StreamUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

@Service
@Slf4j
public class VoucherOrderServiceImpl_kafka extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SeckillMessageMapper seckillMessageMapper;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        // 脚本内容只读一次。setLocation(ClassPathResource) 会让 DefaultRedisScript 在每次执行时
        // 重新读 fat jar 内的资源（getSha1 里的 isModified 检查 + 取脚本字节），读取要过 JarFile 全局锁，
        // 在 Windows 上还会触发文件时间戳检查，实测把所有请求串行化、吞吐压到 ~420/s。
        // setScriptText(StaticScriptSource) 后脚本常驻内存，isModified() 恒为 false，sha1 只算一次。
        SECKILL_SCRIPT.setScriptText(readScriptText("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static String readScriptText(String classpathLocation) {
        try (InputStream in = new ClassPathResource(classpathLocation).getInputStream()) {
            return StreamUtils.copyToString(in, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("加载 Lua 脚本失败: " + classpathLocation, e);
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        if (result == null) {
            // 脚本未执行/连接异常时 execute 返回 null，此时尚未产生任何准入，直接失败即可
            log.error("秒杀 lua 执行返回 null，voucherId={}, userId={}, orderId={}",
                    voucherId, userId, orderId);
            return Result.fail("秒杀服务繁忙，请稍后重试");
        }
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // lua 已把 orderId 原子写入 Redis（seckill:admitted），Redis 即准入真值。
        // 这里落 outbox 只是为了加速投递；即便失败，恢复任务也会依据 Redis 把订单补齐。
        try {
            seckillMessageMapper.insert(new SeckillMessage()
                    .setOrderId(orderId)
                    .setUserId(userId)
                    .setVoucherId(voucherId)
                    .setStatus(SeckillMessage.STATUS_READY)
                    .setRetry(0));
        } catch (Exception e) {
            log.error("秒杀资格落本地消息表失败，将由恢复任务依据 Redis 补单，orderId={}, userId={}, voucherId={}",
                    orderId, userId, voucherId, e);
        }
        log.info("秒杀资格已准入，orderId={}, userId={}, voucherId={}", orderId, userId, voucherId);
        return Result.ok(orderId);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        // 先写订单，以数据库唯一键作为幂等和一人一单的最终闸门
        save(voucherOrder);
        log.info("MySQL 秒杀订单已插入当前事务（待提交），orderId={}, userId={}, voucherId={}",
                voucherOrder.getId(), userId, voucherId);

        // 订单写入成功后再条件扣减库存；失败时抛异常，事务会回滚刚写入的订单
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!success) {
            throw new IllegalStateException(
                    "MySQL 秒杀库存不足或库存数据不存在，voucherId=" + voucherId
                            + ", orderId=" + voucherOrder.getId());
        }
        log.info("MySQL 秒杀库存扣减成功，orderId={}, voucherId={}", voucherOrder.getId(), voucherId);

        // 与订单、库存同一事务提交：status=COMPLETED 表示链路真正走完（SENT 只代表到了 Kafka）
        seckillMessageMapper.update(null,
                new UpdateWrapper<SeckillMessage>()
                        .eq("order_id", voucherOrder.getId())
                        .set("status", SeckillMessage.STATUS_COMPLETED));
        log.info("MySQL 秒杀订单事务处理完成，orderId={}, userId={}, voucherId={}",
                voucherOrder.getId(), userId, voucherId);
    }

    @Override
    public Result createVoucherOrder2(Long voucherId) {
        return Result.ok();
    }
}
