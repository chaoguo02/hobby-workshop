package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.entity.SeckillMessage;
import com.hmdp.mapper.SeckillMessageMapper;
import com.hmdp.monitor.SeckillEventLogger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * 卡死消息对账：处理长时间停在 PROCESSING（relay 抢占后宕机/发送超时）或 SENT
 * （broker 丢消息、消费失败进 DLT、消费者宕机）的消息——这两种都说明链路没走完。
 *
 * 未达告警阈值（{@link SeckillRetryPolicy#ALERT_THRESHOLD}）置回 READY 常规退避重投；
 * 达到阈值置 FAILED 并告警，但 FAILED 不是终态：relay 会继续扫描 FAILED，
 * 按更长的退避自动重投（最长 1h）。因此「每笔准入最终都会自动落库」不依赖人工，
 * 人工重放只是清零退避、加速处理。
 * 重复投递由消费者幂等兜住，所以这里可以放心重投。
 */
@Component
@Slf4j
public class SeckillMessageReconcileTask {

    private static final int BATCH_SIZE = 100;
    private static final long STUCK_TIMEOUT_SECONDS = 60L;

    @Resource
    private SeckillMessageMapper seckillMessageMapper;

    @Resource
    private SeckillEventLogger seckillEventLogger;

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
            if (retry < SeckillRetryPolicy.ALERT_THRESHOLD) {
                long delaySeconds = SeckillRetryPolicy.fastBackoffSeconds(retry);
                int updated = seckillMessageMapper.update(null,
                        new UpdateWrapper<SeckillMessage>()
                                .eq("order_id", message.getOrderId())
                                .eq("status", message.getStatus())
                                .set("status", SeckillMessage.STATUS_READY)
                                .setSql("retry = retry + 1, next_retry_time = DATE_ADD(NOW(), INTERVAL "
                                        + delaySeconds + " SECOND)"));
                if (updated > 0) {
                    seckillEventLogger.warn(message.getOrderId(), message.getUserId(), message.getVoucherId(),
                            SeckillEventLogger.STAGE_RECONCILE_RESET,
                            "卡在 status=" + message.getStatus() + " 超过 " + STUCK_TIMEOUT_SECONDS
                                    + "s，对账任务重置为 READY，退避 " + delaySeconds + "s 重投（第 "
                                    + (retry + 1) + " 次）");
                    log.warn("秒杀消息卡在 status={} 超时，重置为 READY 退避重投，orderId={}, retry={}, delay={}s",
                            message.getStatus(), message.getOrderId(), retry + 1, delaySeconds);
                }
            } else {
                long delaySeconds = SeckillRetryPolicy.slowBackoffSeconds(retry);
                int updated = seckillMessageMapper.update(null,
                        new UpdateWrapper<SeckillMessage>()
                                .eq("order_id", message.getOrderId())
                                .eq("status", message.getStatus())
                                .set("status", SeckillMessage.STATUS_FAILED)
                                .setSql("retry = retry + 1, next_retry_time = DATE_ADD(NOW(), INTERVAL "
                                        + delaySeconds + " SECOND)"));
                if (updated > 0) {
                    seckillEventLogger.error(message.getOrderId(), message.getUserId(), message.getVoucherId(),
                            SeckillEventLogger.STAGE_RECONCILE_FAILED,
                            "[告警] 已重投 " + retry + " 次仍未完成，转入 FAILED 慢速通道（" + delaySeconds
                                    + "s 后自动重试）；也可在监控页人工重放加速");
                    log.error("[告警] 秒杀消息已重投 {} 次仍未完成，转入 FAILED 慢速通道继续自动重试（{}s 后），"
                                    + "orderId={}, userId={}, voucherId={}, fromStatus={}",
                            retry, delaySeconds, message.getOrderId(), message.getUserId(),
                            message.getVoucherId(), message.getStatus());
                }
            }
        }
    }
}
