package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.SeckillMessage;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.mapper.SeckillMessageMapper;
import com.hmdp.service.ISeckillVoucherService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * 库存数字级对账：以 Redis 为真值核对 MySQL 库存有没有跟上，只检测告警、不自动改数。
 *
 * 恒等式推导：Redis 在准入时扣库存，MySQL 只在订单事务提交时扣库存，
 * 于是「MySQL 库存 = Redis 库存 + 已准入但订单尚未落库的数量」。
 * 每个准入都会有一条本地消息，COMPLETED 代表订单+扣库存已同事务提交，
 * 所以「订单未落库的数量」= 该券下状态非 COMPLETED 的消息数。
 * 两边对不上说明 Redis 扣了 MySQL 没扣（消息卡住/FAILED/丢失）或 MySQL 扣多了（重复下单类 bug）。
 */
@Component
@Slf4j
public class SeckillStockReconcileTask {

    private static final String STOCK_KEY_PREFIX = "seckill:stock:";
    private static final int SCAN_COUNT = 1000;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private SeckillMessageMapper seckillMessageMapper;

    @Scheduled(fixedDelay = 60000)
    public void reconcile() {
        int checked = 0;
        int drifted = 0;
        for (String key : scanKeys(STOCK_KEY_PREFIX + "*")) {
            Long voucherId;
            try {
                voucherId = Long.valueOf(key.substring(STOCK_KEY_PREFIX.length()));
            } catch (NumberFormatException e) {
                continue;
            }
            Long redisStock = stockOf(key);
            if (redisStock == null) {
                continue;
            }
            SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
            if (voucher == null || voucher.getStock() == null) {
                log.warn("[对账] Redis 有库存 key 但 MySQL 无对应秒杀券，voucherId={}", voucherId);
                continue;
            }
            checked++;
            int notCommitted = seckillMessageMapper.selectCount(
                    new QueryWrapper<SeckillMessage>()
                            .eq("voucher_id", voucherId)
                            .ne("status", SeckillMessage.STATUS_COMPLETED));
            long expectedMysqlStock = redisStock + notCommitted;
            if (voucher.getStock() != expectedMysqlStock) {
                drifted++;
                log.error("[对账告警] 库存数字漂移，voucherId={}：Redis库存={}, 未落库准入={}, 预期MySQL库存={}, 实际MySQL库存={}, 差值={}",
                        voucherId, redisStock, notCommitted, expectedMysqlStock, voucher.getStock(),
                        voucher.getStock() - expectedMysqlStock);
            }
        }
        if (drifted > 0) {
            log.error("[对账告警] 本轮核对 {} 张券，{} 张库存漂移，需人工核查", checked, drifted);
        } else if (checked > 0) {
            log.info("[对账] 本轮核对 {} 张券，库存一致", checked);
        }
    }

    private Long stockOf(String key) {
        String value = stringRedisTemplate.opsForValue().get(key);
        if (value == null) {
            return null;
        }
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException e) {
            log.warn("[对账] 库存值非法，key={}, value={}", key, value);
            return null;
        }
    }

    /** SCAN 游标遍历，避免 KEYS 阻塞 Redis 主线程 */
    private Set<String> scanKeys(String pattern) {
        return stringRedisTemplate.execute((RedisCallback<Set<String>>) connection -> {
            Set<String> keys = new HashSet<>();
            ScanOptions options = ScanOptions.scanOptions().match(pattern).count(SCAN_COUNT).build();
            try (Cursor<byte[]> cursor = connection.scan(options)) {
                while (cursor.hasNext()) {
                    keys.add(new String(cursor.next(), StandardCharsets.UTF_8));
                }
            }
            return keys;
        });
    }
}
