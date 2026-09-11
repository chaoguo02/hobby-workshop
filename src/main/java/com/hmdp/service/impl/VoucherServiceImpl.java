package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.constants.RedisConstants.SECKILL_ADMITTED_KEY;
import static com.hmdp.constants.RedisConstants.SECKILL_ORDER_KEY;
import static com.hmdp.constants.RedisConstants.SECKILL_STOCK_KEY;

@Service
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements IVoucherService {

    /** 活动结束后库存 key 的保留缓冲：本项目的 Lua 不校验时间，结束后仍可能继续卖到库存耗尽 */
    private static final long STOCK_TTL_BUFFER_SECONDS = TimeUnit.DAYS.toSeconds(3);
    private static final long MIN_STOCK_TTL_SECONDS = TimeUnit.HOURS.toSeconds(1);

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryVoucherOfShop(Long shopId) {
        // 查询优惠券信息
        List<Voucher> vouchers = getBaseMapper().queryVoucherOfShop(shopId);
        // 返回结果
        return Result.ok(vouchers);
    }

    @Override
    @Transactional
    public void addSeckillVoucher(Voucher voucher) {
        // 保存优惠券
        save(voucher);
        // 保存秒杀信息
        SeckillVoucher seckillVoucher = new SeckillVoucher();
        seckillVoucher.setVoucherId(voucher.getId());
        seckillVoucher.setStock(voucher.getStock());
        seckillVoucher.setBeginTime(voucher.getBeginTime());
        seckillVoucher.setEndTime(voucher.getEndTime());
        seckillVoucherService.save(seckillVoucher);
        // 保存秒杀的库存到redis中；带 TTL（活动结束 + 缓冲），一人一单集合在 Lua 里跟随该 TTL 过期，防止无限增长
        long ttlSeconds = stockTtlSeconds(voucher.getEndTime());
        stringRedisTemplate.opsForValue().set(SECKILL_STOCK_KEY + voucher.getId(),
                voucher.getStock().toString(), ttlSeconds, TimeUnit.SECONDS);
        log.debug("将秒杀的库存保存到redis中，ttl=" + ttlSeconds + "s");
    }

    /** 库存 key 的 TTL：活动剩余时间 + 缓冲，且有下限；Lua 不校验活动时间，缓冲要覆盖结束后仍可能卖光的情况 */
    private long stockTtlSeconds(LocalDateTime endTime) {
        long ttlSeconds = STOCK_TTL_BUFFER_SECONDS;
        if (endTime != null) {
            ttlSeconds += Duration.between(LocalDateTime.now(), endTime).getSeconds();
        }
        return Math.max(ttlSeconds, MIN_STOCK_TTL_SECONDS);
    }

    @Override
    public Result listSeckillVouchers() {
        List<SeckillVoucher> seckillVouchers = seckillVoucherService.list();
        List<Map<String, Object>> rows = new ArrayList<>(seckillVouchers.size());
        for (SeckillVoucher seckillVoucher : seckillVouchers) {
            Long voucherId = seckillVoucher.getVoucherId();
            Voucher voucher = getById(voucherId);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("voucherId", voucherId);
            row.put("title", voucher == null ? null : voucher.getTitle());
            row.put("shopId", voucher == null ? null : voucher.getShopId());
            row.put("mysqlStock", seckillVoucher.getStock());
            row.put("redisStock", stringRedisTemplate.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
            row.put("orderedUsers", stringRedisTemplate.opsForSet().size(SECKILL_ORDER_KEY + voucherId));
            row.put("beginTime", seckillVoucher.getBeginTime());
            row.put("endTime", seckillVoucher.getEndTime());
            rows.add(row);
        }
        return Result.ok(rows);
    }

    @Override
    @Transactional
    public Result updateSeckillStock(Long voucherId, Integer stock) {
        if (stock == null || stock < 0) {
            return Result.fail("库存必须为非负数");
        }
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        if (seckillVoucher == null) {
            return Result.fail("秒杀券不存在");
        }
        seckillVoucherService.update(new UpdateWrapper<SeckillVoucher>()
                .eq("voucher_id", voucherId)
                .set("stock", stock));
        // Redis 是准入真值，改库存必须同步，否则 Lua 仍按旧值放行
        stringRedisTemplate.opsForValue().set(SECKILL_STOCK_KEY + voucherId,
                stock.toString(), stockTtlSeconds(seckillVoucher.getEndTime()), TimeUnit.SECONDS);
        return Result.ok(stock);
    }

    @Override
    public Result resetSeckillOrders(Long voucherId) {
        Long previous = stringRedisTemplate.opsForSet().size(SECKILL_ORDER_KEY + voucherId);
        stringRedisTemplate.delete(Arrays.asList(
                SECKILL_ORDER_KEY + voucherId,
                SECKILL_ADMITTED_KEY + voucherId));
        return Result.ok(previous == null ? 0 : previous);
    }

    @Override
    @Transactional
    public Result removeSeckillVoucher(Long voucherId) {
        seckillVoucherService.removeById(voucherId);
        removeById(voucherId);
        stringRedisTemplate.delete(Arrays.asList(
                SECKILL_STOCK_KEY + voucherId,
                SECKILL_ORDER_KEY + voucherId,
                SECKILL_ADMITTED_KEY + voucherId));
        return Result.ok();
    }

}
