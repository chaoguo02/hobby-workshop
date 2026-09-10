package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
public class VoucherOrderServiceImpl_kafka extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private static final String VOUCHER_ORDER_TOPIC = "voucher-orders";
    private static final long KAFKA_SEND_TIMEOUT_SECONDS = 3L;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private KafkaTemplate<String, VoucherOrder> kafkaTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
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
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        String messageKey = voucherId.toString();
        ListenableFuture<SendResult<String, VoucherOrder>> sendFuture =
                kafkaTemplate.send(VOUCHER_ORDER_TOPIC, messageKey, voucherOrder);
        try {
            SendResult<String, VoucherOrder> sendResult =
                    sendFuture.get(KAFKA_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("秒杀订单已写入 Kafka，topic={}, partition={}, offset={}, key={}, orderId={}",
                    sendResult.getRecordMetadata().topic(),
                    sendResult.getRecordMetadata().partition(),
                    sendResult.getRecordMetadata().offset(),
                    messageKey,
                    orderId);
            return Result.ok(orderId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("等待 Kafka 发送结果时线程被中断，orderId={}", orderId, e);
            return Result.fail("秒杀请求提交失败，请稍后重试");
        } catch (ExecutionException | TimeoutException e) {
            log.error("秒杀订单发送 Kafka 失败，orderId={}, userId={}, voucherId={}",
                    orderId, userId, voucherId, e);
            return Result.fail("秒杀请求提交失败，请稍后重试");
        }
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
        log.info("MySQL 秒杀订单事务处理完成，orderId={}, userId={}, voucherId={}",
                voucherOrder.getId(), userId, voucherId);
    }

    @Override
    public Result createVoucherOrder2(Long voucherId) {
        return Result.ok();
    }
}
