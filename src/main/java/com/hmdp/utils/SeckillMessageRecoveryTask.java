package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.SeckillMessage;
import com.hmdp.mapper.SeckillMessageMapper;
import com.hmdp.monitor.SeckillEventLogger;
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

    @Resource
    private SeckillEventLogger seckillEventLogger;

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
        // 提取 voucherId
        Long voucherId;
        try {
            voucherId = Long.valueOf(key.substring(ADMITTED_KEY_PREFIX.length()));
        } catch (NumberFormatException e) {
            log.warn("无法从 key 解析 voucherId，跳过：{}", key);
            return;
        }
        // 用 HSCAN 游标分批遍历：HGETALL 在热点券下会把整个 Hash（可能几十万 field）一次性拉进内存，
        // 这里每轮只驻留 CHUNK_SIZE 条 orderId -> userId，处理完立即释放。
        ScanOptions options = ScanOptions.scanOptions().count(SCAN_COUNT).build();
        List<Long> orderIds = new ArrayList<>(CHUNK_SIZE);
        Map<Long, Long> userByOrder = new HashMap<>(CHUNK_SIZE);
        List<String> completedFields = new ArrayList<>();
        try (Cursor<Map.Entry<Object, Object>> cursor = stringRedisTemplate.opsForHash().scan(key, options)) {
            while (cursor.hasNext()) {
                Map.Entry<Object, Object> entry = cursor.next();
                Long orderId = Long.valueOf(entry.getKey().toString());
                orderIds.add(orderId);
                userByOrder.put(orderId, Long.valueOf(entry.getValue().toString()));
                if (orderIds.size() >= CHUNK_SIZE) {
                    processBatch(orderIds, userByOrder, voucherId, completedFields);
                    orderIds.clear();
                    userByOrder.clear();
                }
            }
        }
        if (!orderIds.isEmpty()) {
            processBatch(orderIds, userByOrder, voucherId, completedFields);
        }
        // 游标关闭后再删，避免在 HSCAN 迭代过程中改 Hash；分批删以防单条命令参数过多
        for (int start = 0; start < completedFields.size(); start += CHUNK_SIZE) {
            List<String> slice = completedFields.subList(
                    start, Math.min(start + CHUNK_SIZE, completedFields.size()));
            stringRedisTemplate.opsForHash().delete(key, slice.toArray());
            for (String field : slice) {
                try {
                    seckillEventLogger.info(Long.valueOf(field), null, voucherId,
                            SeckillEventLogger.STAGE_RECOVERY_CLEANUP,
                            "订单已落库，回收 Redis seckill:admitted 准入记录，链路完全收尾");
                } catch (NumberFormatException ignored) {
                    // field 非数字，跳过埋点
                }
            }
        }
    }

    /** 核对一批已准入 orderId：MySQL 缺失的补插 READY，已 COMPLETED 的登记待清理 */
    private void processBatch(List<Long> orderIds, Map<Long, Long> userByOrder,
                              Long voucherId, List<String> completedFields) {
        List<SeckillMessage> rows = seckillMessageMapper.selectList(
                new QueryWrapper<SeckillMessage>().in("order_id", orderIds));
        Set<Long> existing = new HashSet<>();
        for (SeckillMessage row : rows) {
            existing.add(row.getOrderId());
            if (row.getStatus() != null && row.getStatus() == SeckillMessage.STATUS_COMPLETED) {
                completedFields.add(row.getOrderId().toString());
            }
        }
        for (Long orderId : orderIds) {
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
                seckillEventLogger.warn(orderId, userByOrder.get(orderId), voucherId,
                        SeckillEventLogger.STAGE_RECOVERY_BACKFILL,
                        "Redis 已准入但 outbox 缺失（请求线程写库失败），恢复任务补插 READY，重新走投递");
                log.warn("依据 Redis 准入记录补齐缺失的本地消息，orderId={}, userId={}, voucherId={}",
                        orderId, userByOrder.get(orderId), voucherId);
            } catch (DuplicateKeyException e) {
                // 并发下已被其它线程补齐，忽略
            }
        }
    }
}
