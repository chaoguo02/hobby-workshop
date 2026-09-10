package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.entity.SeckillMessage;
import com.hmdp.mapper.SeckillMessageMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * FAILED 消息的人工重放入口。FAILED 是重投多次仍失败的终态，本来只告警、只能人工改库；
 * 这里把 FAILED 条件重置为 READY（retry 归零、清退避），交回 relay 正常投递。
 * 只允许从 FAILED 迁移，避免误动在途（READY/PROCESSING/SENT）的消息。
 */
@Component
@Slf4j
public class SeckillMessageReplayService {

    private static final int MAX_BATCH = 500;

    @Resource
    private SeckillMessageMapper seckillMessageMapper;

    /** 单条重放；返回是否命中并重置了一条 FAILED 消息 */
    public boolean replay(Long orderId) {
        int updated = seckillMessageMapper.update(null,
                new UpdateWrapper<SeckillMessage>()
                        .eq("order_id", orderId)
                        .eq("status", SeckillMessage.STATUS_FAILED)
                        .set("status", SeckillMessage.STATUS_READY)
                        .set("retry", 0)
                        .set("next_retry_time", null));
        if (updated > 0) {
            log.warn("人工重放：FAILED -> READY，orderId={}", orderId);
            return true;
        }
        log.info("人工重放未命中（orderId={} 不是 FAILED 状态），忽略", orderId);
        return false;
    }

    /** 按最老优先批量重放 FAILED，返回实际重置条数 */
    public int replayAllFailed(int limit) {
        int capped = Math.max(1, Math.min(limit, MAX_BATCH));
        List<SeckillMessage> failed = seckillMessageMapper.selectList(
                new QueryWrapper<SeckillMessage>()
                        .eq("status", SeckillMessage.STATUS_FAILED)
                        .orderByAsc("update_time")
                        .last("limit " + capped));
        int replayed = 0;
        for (SeckillMessage message : failed) {
            if (replay(message.getOrderId())) {
                replayed++;
            }
        }
        log.warn("人工批量重放：扫描 {} 条 FAILED，重置 {} 条回 READY", failed.size(), replayed);
        return replayed;
    }
}
