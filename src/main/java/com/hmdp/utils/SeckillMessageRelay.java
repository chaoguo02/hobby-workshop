package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.entity.SeckillMessage;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillMessageMapper;
import com.hmdp.monitor.SeckillEventLogger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.concurrent.ListenableFuture;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 本地消息表投递器：定时扫描待发送记录，投递 Kafka 成功回调里标记 SENT。
 * 先在一个事务里批量条件 UPDATE 抢占为 PROCESSING（多实例并行时只有抢到的那一个会发送）；
 * 发送走异步回调，不再用 get() 阻塞 relay 线程，单轮可派发整批消息。
 * 投递失败按退避策略置回 READY（常规段）或 FAILED（告警段）并设置 next_retry_time。
 *
 * 同时扫描 READY 与 FAILED：FAILED 不是终态，只是退避更长的慢速通道，
 * 到 next_retry_time 后仍会被这里自动重投，人工重放仅用于加速。
 */
@Component
@Slf4j
public class SeckillMessageRelay {

    private static final String VOUCHER_ORDER_TOPIC = "voucher-orders";
    private static final int BATCH_SIZE = 200;

    @Resource
    private SeckillMessageMapper seckillMessageMapper;

    @Resource
    private KafkaTemplate<String, VoucherOrder> kafkaTemplate;

    @Resource
    private PlatformTransactionManager transactionManager;

    @Resource
    private SeckillEventLogger seckillEventLogger;

    @Scheduled(fixedDelay = 500)
    public void relay() {
        List<SeckillMessage> candidates = seckillMessageMapper.selectList(
                new QueryWrapper<SeckillMessage>()
                        .in("status", SeckillMessage.STATUS_READY, SeckillMessage.STATUS_FAILED)
                        .apply("(next_retry_time IS NULL OR next_retry_time <= NOW())")
                        .orderByAsc("create_time")
                        .last("limit " + BATCH_SIZE)
        );
        long start = System.currentTimeMillis();
        long claimNanos = System.nanoTime();
        List<SeckillMessage> claimed = claimBatch(candidates);
        claimNanos = System.nanoTime() - claimNanos;
        for (SeckillMessage message : claimed) {
            int retryNo = (message.getRetry() == null ? 0 : message.getRetry()) + 1;
            seckillEventLogger.info(message.getOrderId(), message.getUserId(), message.getVoucherId(),
                    SeckillEventLogger.STAGE_RELAY_CLAIMED,
                    "relay 抢占为 PROCESSING（第 " + retryNo + " 次投递），准备发送 Kafka");
            dispatch(message);
        }
        if (!claimed.isEmpty()) {
            log.info("relay 本轮扫描 {} 条、派发 {} 条，抢占 {} ms、总耗时 {} ms",
                    candidates.size(), claimed.size(),
                    claimNanos / 1_000_000,
                    System.currentTimeMillis() - start);
        }
    }

    /**
     * 单事务内批量抢占：把 N 条 READY→PROCESSING 合并为一次提交，
     * 避免逐条 autocommit 时每条一次 redo fsync（实测 100 条约 900ms）。
     * 事务只做本地 UPDATE，不做任何网络 I/O；提交后由调用方异步投递。
     * 多实例并发时后到的实例在行锁释放后条件不成立、影响行数为 0，仍只有抢到的那一个会发送。
     */
    private List<SeckillMessage> claimBatch(List<SeckillMessage> candidates) {
        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        List<SeckillMessage> claimed = template.execute(status -> {
            List<SeckillMessage> result = new ArrayList<>();
            for (SeckillMessage message : candidates) {
                int updated = seckillMessageMapper.update(null,
                        new UpdateWrapper<SeckillMessage>()
                                .eq("order_id", message.getOrderId())
                                .in("status", SeckillMessage.STATUS_READY, SeckillMessage.STATUS_FAILED)
                                .set("status", SeckillMessage.STATUS_PROCESSING));
                if (updated > 0) {
                    result.add(message);
                }
            }
            return result;
        });
        return claimed == null ? Collections.emptyList() : claimed;
    }

    private void dispatch(SeckillMessage message) {
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(message.getOrderId());
        voucherOrder.setUserId(message.getUserId());
        voucherOrder.setVoucherId(message.getVoucherId());
        // 抢券成功先占用名额并生成待支付订单；支付完成后才产生核销资格。
        voucherOrder.setStatus(VoucherOrder.STATUS_PENDING_PAYMENT);
        ListenableFuture<SendResult<String, VoucherOrder>> future;
        try {
            // 以 orderId 作为 key：订单之间互相独立（主键 + uk_user_voucher + Redis 一人一单），
            // 不需要按 voucherId 保序；用 orderId 打散可让同一张热门券的消息分布到多个分区并行消费。
            future = kafkaTemplate.send(
                    VOUCHER_ORDER_TOPIC, message.getOrderId().toString(), voucherOrder);
        } catch (Exception e) {
            log.error("本地消息发送失败（同步抛错），退避后重试，orderId={}", message.getOrderId(), e);
            releaseForRetry(message, e.getMessage());
            return;
        }
        future.addCallback(
                sendResult -> {
                    int updated = seckillMessageMapper.update(null,
                            new UpdateWrapper<SeckillMessage>()
                                    .eq("order_id", message.getOrderId())
                                    .eq("status", SeckillMessage.STATUS_PROCESSING)
                                    .set("status", SeckillMessage.STATUS_SENT));
                    if (updated > 0) {
                        seckillEventLogger.info(message.getOrderId(), message.getUserId(), message.getVoucherId(),
                                SeckillEventLogger.STAGE_KAFKA_SENT,
                                "已送达 Kafka（partition=" + sendResult.getRecordMetadata().partition()
                                        + ", offset=" + sendResult.getRecordMetadata().offset() + "）");
                        log.info("本地消息投递成功，orderId={}, partition={}, offset={}",
                                message.getOrderId(),
                                sendResult.getRecordMetadata().partition(),
                                sendResult.getRecordMetadata().offset());
                    }
                },
                ex -> {
                    log.error("本地消息投递失败，退避后重试，orderId={}", message.getOrderId(), ex);
                    releaseForRetry(message, ex.getMessage());
                });
    }

    /**
     * 投递失败：从 PROCESSING 置回 READY（常规段）或 FAILED（告警段），retry+1，
     * 并按退避策略设置 next_retry_time。达到告警阈值后走慢速通道但不会停止重试。
     */
    private void releaseForRetry(SeckillMessage message, String errorMessage) {
        int retry = message.getRetry() == null ? 0 : message.getRetry();
        long delaySeconds = SeckillRetryPolicy.backoffSeconds(retry);
        int nextStatus = SeckillRetryPolicy.statusForRetry(retry);
        int updated = seckillMessageMapper.update(null,
                new UpdateWrapper<SeckillMessage>()
                        .eq("order_id", message.getOrderId())
                        .eq("status", SeckillMessage.STATUS_PROCESSING)
                        .set("status", nextStatus)
                        .setSql("retry = retry + 1, next_retry_time = DATE_ADD(NOW(), INTERVAL "
                                + delaySeconds + " SECOND)"));
        if (updated > 0) {
            seckillEventLogger.warn(message.getOrderId(), message.getUserId(), message.getVoucherId(),
                    SeckillEventLogger.STAGE_RELAY_SEND_FAILED,
                    "第 " + (retry + 1) + " 次投递 Kafka 失败，置回 " + statusLabel(nextStatus)
                            + "，退避 " + delaySeconds + "s 后自动重试；错误：" + errorMessage);
        }
    }

    private String statusLabel(int status) {
        return status == SeckillMessage.STATUS_FAILED ? "FAILED(慢速通道)" : "READY";
    }
}
