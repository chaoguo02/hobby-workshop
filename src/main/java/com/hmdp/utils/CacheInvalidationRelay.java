package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.entity.CacheInvalidationEvent;
import com.hmdp.mapper.CacheInvalidationEventMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/** 将事务内记录的缓存失效事件可靠执行；失败记录不会丢失，应用重启后仍可继续重试。 */
@Component
@Slf4j
public class CacheInvalidationRelay {

    private static final int BATCH_SIZE = 100;
    private static final long MAX_BACKOFF_SECONDS = 300L;

    @Resource
    private CacheInvalidationEventMapper eventMapper;

    @Resource
    private CacheClient cacheClient;

    /** 事务提交后调用的快速路径；失败只记录，后续由定时扫描恢复。 */
    public void deliverNow(Long eventId) {
        if (eventId == null) {
            return;
        }
        try {
            CacheInvalidationEvent event = eventMapper.selectById(eventId);
            if (event != null && event.getStatus() == CacheInvalidationEvent.STATUS_PENDING) {
                deliver(event);
            }
        } catch (RuntimeException e) {
            // 业务事务已经提交，不能再向调用方伪装成“更新失败”；事件仍在库中，扫描任务会恢复。
            log.error("事务提交后的缓存立即失效尝试异常，将由定时任务恢复，eventId={}", eventId, e);
        }
    }

    @Scheduled(fixedDelayString = "${cache.invalidation.retry-interval-ms:5000}")
    public void retryPending() {
        List<CacheInvalidationEvent> pending = eventMapper.selectList(
                new QueryWrapper<CacheInvalidationEvent>()
                        .eq("status", CacheInvalidationEvent.STATUS_PENDING)
                        .apply("(next_retry_time IS NULL OR next_retry_time <= NOW())")
                        .orderByAsc("create_time")
                        .last("limit " + BATCH_SIZE));
        for (CacheInvalidationEvent event : pending) {
            deliver(event);
        }
    }

    private void deliver(CacheInvalidationEvent event) {
        try {
            cacheClient.invalidate(event.getCacheKey(), event.getLockKey());
            eventMapper.update(null,
                    new UpdateWrapper<CacheInvalidationEvent>()
                            .eq("id", event.getId())
                            .eq("status", CacheInvalidationEvent.STATUS_PENDING)
                            .set("status", CacheInvalidationEvent.STATUS_COMPLETED)
                            .set("last_error", null));
            log.info("缓存可靠失效完成，eventId={}, key={}", event.getId(), event.getCacheKey());
        } catch (RuntimeException e) {
            int retry = event.getRetry() == null ? 0 : event.getRetry();
            long delay = Math.min(MAX_BACKOFF_SECONDS, 1L << Math.min(retry, 8));
            String error = e.getMessage();
            if (error != null && error.length() > 500) {
                error = error.substring(0, 500);
            }
            eventMapper.update(null,
                    new UpdateWrapper<CacheInvalidationEvent>()
                            .eq("id", event.getId())
                            .eq("status", CacheInvalidationEvent.STATUS_PENDING)
                            .setSql("retry = retry + 1, next_retry_time = DATE_ADD(NOW(), INTERVAL "
                                    + delay + " SECOND)")
                            .set("last_error", error));
            log.error("缓存失效失败，保留事件等待重试，eventId={}, key={}, retry={}",
                    event.getId(), event.getCacheKey(), retry + 1, e);
        }
    }

    /** 清理已完成历史记录，保留七天用于排障。 */
    @Scheduled(cron = "${cache.invalidation.cleanup-cron:0 20 3 * * ?}")
    public void cleanupCompleted() {
        eventMapper.delete(new QueryWrapper<CacheInvalidationEvent>()
                .eq("status", CacheInvalidationEvent.STATUS_COMPLETED)
                .apply("update_time < DATE_SUB(NOW(), INTERVAL 7 DAY)"));
    }
}
