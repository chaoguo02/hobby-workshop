package com.hmdp.service.impl;

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
import java.util.List;
import java.util.concurrent.TimeUnit;

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
        long ttlSeconds = STOCK_TTL_BUFFER_SECONDS;
        if (voucher.getEndTime() != null) {
            ttlSeconds += Duration.between(LocalDateTime.now(), voucher.getEndTime()).getSeconds();
        }
        ttlSeconds = Math.max(ttlSeconds, MIN_STOCK_TTL_SECONDS);
        stringRedisTemplate.opsForValue().set(SECKILL_STOCK_KEY + voucher.getId(),
                voucher.getStock().toString(), ttlSeconds, TimeUnit.SECONDS);
        log.debug("将秒杀的库存保存到redis中，ttl=" + ttlSeconds + "s");
    }

}
