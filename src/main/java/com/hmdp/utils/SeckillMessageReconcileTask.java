package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.entity.SeckillMessage;
import com.hmdp.mapper.SeckillMessageMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * 卡死消息对账：处理长时间停在 PROCESSING（relay 抢占后宕机/发送超时）或 SENT
 * （broker 丢消息、消费失败进 DLT、消费者宕机）的消息——这两种都说明链路没走完。
 * 未超重投上限则置回 READY 并退避重投；超上限置 FAILED 告警转人工。
 * 重复投递由消费者幂等兜住，所以这里可以放心重投。
 */
@Component
@Slf4j
public class SeckillMessageReconcileTask {

    private static final int BATCH_SIZE = 100;
    private static final int MAX_REDELIVER = 3;
    private static final long STUCK_TIMEOUT_SECONDS = 60L;
    private static final long MAX_BACKOFF_SECONDS = 300L;

    @Resource
    private SeckillMessageMapper seckillMessageMapper;

    @Scheduled(fixedDelay = 30000)
    public void reconcile() {
        List<SeckillMessage> stuck = seckillMessageMapper.selectList(
                new QueryWrapper<SeckillMessage>()
                        .in("status", SeckillMessage.STATUS_PROCESSING, SeckillMessage.STATUS_SENT)
                        .apply("update_time < DATE_SUB(NOW(), INTERVAL {0} SECOND)", STUCK_TIMEOUT_SECONDS)
                        .orderByAsc("update_time")
                        .last("limit " + BATCH_SIZE)
        );
        for (SeckillMessage message : stuck) {
            int retry = message.getRetry() == null ? 0 : message.getRetry();
            if (retry >= MAX_REDELIVER) {
                int updated = seckillMessageMapper.update(null,
                        new UpdateWrapper<SeckillMessage>()
                                .eq("order_id", message.getOrderId())
                                .eq("status", message.getStatus())
                                .set("status", SeckillMessage.STATUS_FAILED));
                if (updated > 0) {
                    log.error("[告警] 秒杀消息重投 {} 次仍未完成，置 FAILED 待人工处理，"
                                    + "orderId={}, userId={}, voucherId={}, fromStatus={}",
                            retry, message.getOrderId(), message.getUserId(),
                            message.getVoucherId(), message.getStatus());
                }
            } else {
                long delaySeconds = Math.min(MAX_BACKOFF_SECONDS, 1L << Math.min(retry, 8));
                int updated = seckillMessageMapper.update(null,
                        new UpdateWrapper<SeckillMessage>()
                                .eq("order_id", message.getOrderId())
                                .eq("status", message.getStatus())
                                .set("status", SeckillMessage.STATUS_READY)
                                .setSql("retry = retry + 1, next_retry_time = DATE_ADD(NOW(), INTERVAL "
                                        + delaySeconds + " SECOND)"));
                if (updated > 0) {
                    log.warn("秒杀消息卡在 status={} 超时，重置为 READY 退避重投，orderId={}, retry={}, delay={}s",
                            message.getStatus(), message.getOrderId(), retry + 1, delaySeconds);
                }
            }
        }
    }
}
