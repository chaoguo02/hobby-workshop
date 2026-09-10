package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.SeckillMessage;
import com.hmdp.mapper.SeckillMessageMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 准入记录恢复任务：Redis 是准入真值，MySQL 必须向 Redis 收敛。
 * 扫描 Redis 里已准入的 orderId，凡是 MySQL outbox 缺失的补插 READY 交给正常链路；
 * 已 COMPLETED 的从 Redis 移除，避免 admitted 结构无限增长。
 */
@Component
@Slf4j
public class SeckillMessageRecoveryTask {

    private static final String ADMITTED_KEY_PREFIX = "seckill:admitted:";
    private static final int CHUNK_SIZE = 500;
    private static final int SCAN_COUNT = 1000;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SeckillMessageMapper seckillMessageMapper;

    @Scheduled(fixedDelay = 30000)
    public void recover() {
        Set<String> keys = scanKeys(ADMITTED_KEY_PREFIX + "*");
        if (keys.isEmpty()) {
            return;
        }
        for (String key : keys) {
            recoverVoucher(key);
        }
    }

    /** 用 SCAN 游标遍历，避免 KEYS 阻塞 Redis 主线程 */
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

    private void recoverVoucher(String key) {
        Map<Object, Object> admitted = stringRedisTemplate.opsForHash().entries(key);
        if (admitted.isEmpty()) {
            return;
        }
        // 提取 voucherId 与 orderId -> userId 映射
        Long voucherId;
        try {
            voucherId = Long.valueOf(key.substring(ADMITTED_KEY_PREFIX.length()));
        } catch (NumberFormatException e) {
            log.warn("无法从 key 解析 voucherId，跳过：{}", key);
            return;
        }
        List<Long> orderIds = new ArrayList<>(admitted.size());
        Map<Long, Long> userByOrder = new HashMap<>(admitted.size());
        for (Map.Entry<Object, Object> entry : admitted.entrySet()) {
            Long orderId = Long.valueOf(entry.getKey().toString());
            orderIds.add(orderId);
            userByOrder.put(orderId, Long.valueOf(entry.getValue().toString()));
        }

        List<String> completedFields = new ArrayList<>();
        for (int start = 0; start < orderIds.size(); start += CHUNK_SIZE) {
            List<Long> slice = orderIds.subList(start, Math.min(start + CHUNK_SIZE, orderIds.size()));
            List<SeckillMessage> rows = seckillMessageMapper.selectList(
                    new QueryWrapper<SeckillMessage>().in("order_id", slice));
            Set<Long> existing = new HashSet<>();
            for (SeckillMessage row : rows) {
                existing.add(row.getOrderId());
                if (row.getStatus() != null && row.getStatus() == SeckillMessage.STATUS_COMPLETED) {
                    completedFields.add(row.getOrderId().toString());
                }
            }
            for (Long orderId : slice) {
                if (existing.contains(orderId)) {
                    continue;
                }
                try {
                    seckillMessageMapper.insert(new SeckillMessage()
                            .setOrderId(orderId)
                            .setUserId(userByOrder.get(orderId))
                            .setVoucherId(voucherId)
                            .setStatus(SeckillMessage.STATUS_READY)
                            .setRetry(0));
                    log.warn("依据 Redis 准入记录补齐缺失的本地消息，orderId={}, userId={}, voucherId={}",
                            orderId, userByOrder.get(orderId), voucherId);
                } catch (DuplicateKeyException e) {
                    // 并发下已被其它线程补齐，忽略
                }
            }
        }
        if (!completedFields.isEmpty()) {
            stringRedisTemplate.opsForHash().delete(key, completedFields.toArray());
        }
    }
}
