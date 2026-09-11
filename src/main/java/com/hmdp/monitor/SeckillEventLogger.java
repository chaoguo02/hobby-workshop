package com.hmdp.monitor;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.SeckillMessageEvent;
import com.hmdp.mapper.SeckillMessageEventMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * 秒杀链路时间线埋点器：在链路每个关键节点写一条事件，供管理后台按订单还原全过程。
 *
 * 三条铁律：
 * 1. 只插不改（append-only），id 自增即时间线顺序；
 * 2. best-effort：观测写入失败只告警，绝不能影响秒杀业务本身；
 * 3. 可开关：{@code seckill.timeline.enabled=false} 时全部 no-op，压测时可关掉避免多一次写。
 *
 * 注意：事件是独立连接写入，不参与业务事务。因此业务成功事件必须等事务提交后再记
 * （见 VoucherOrderConsumer 在 createVoucherOrder 返回后记录 ORDER_COMMITTED），
 * 否则事务回滚会留下假的成功事件。
 */
@Component
@Slf4j
public class SeckillEventLogger {

    /* ---------------- 阶段码：时间线从左到右的顺序 ---------------- */
    /** Redis 预扣库存 + 一人一单通过（Lua 原子完成） */
    public static final String STAGE_REDIS_ADMIT = "REDIS_ADMIT";
    /** 写入本地消息表(outbox)，等 relay 投递 */
    public static final String STAGE_OUTBOX_WRITTEN = "OUTBOX_WRITTEN";
    /** 写 outbox 失败：Redis 已是准入真值，改由恢复任务补 */
    public static final String STAGE_OUTBOX_WRITE_FAILED = "OUTBOX_WRITE_FAILED";
    /** relay 单事务批量抢占成功，准备投递 */
    public static final String STAGE_RELAY_CLAIMED = "RELAY_CLAIMED";
    /** 已投递到 Kafka（含 partition / offset） */
    public static final String STAGE_KAFKA_SENT = "KAFKA_SENT";
    /** 投递 Kafka 失败，退避后重试 */
    public static final String STAGE_RELAY_SEND_FAILED = "RELAY_SEND_FAILED";
    /** 消费者收到 Kafka 消息 */
    public static final String STAGE_CONSUMER_RECEIVED = "CONSUMER_RECEIVED";
    /** 订单写入 MySQL + 扣减库存，同事务提交（链路终点） */
    public static final String STAGE_ORDER_COMMITTED = "ORDER_COMMITTED";
    /** 重复投递，命中已有订单，幂等跳过 */
    public static final String STAGE_CONSUMER_DUPLICATE = "CONSUMER_DUPLICATE";
    /** 唯一键冲突但本 orderId 未落库（数据冲突，不能当成功） */
    public static final String STAGE_CONSUMER_CONFLICT = "CONSUMER_CONFLICT";
    /** 消费抛出异常，将重试 / 进 DLT */
    public static final String STAGE_CONSUMER_FAILED = "CONSUMER_FAILED";
    /** 恢复任务：依据 Redis 准入记录补齐缺失的 outbox 行 */
    public static final String STAGE_RECOVERY_BACKFILL = "RECOVERY_BACKFILL";
    /** 恢复任务：订单已落库，回收 Redis admitted 记录 */
    public static final String STAGE_RECOVERY_CLEANUP = "RECOVERY_CLEANUP";
    /** 对账任务：卡死超时，重置 READY 退避重投 */
    public static final String STAGE_RECONCILE_RESET = "RECONCILE_RESET";
    /** 对账任务：重试耗尽，转 FAILED 慢速通道 */
    public static final String STAGE_RECONCILE_FAILED = "RECONCILE_FAILED";
    /** 人工重放：FAILED -> READY */
    public static final String STAGE_MANUAL_REPLAY = "MANUAL_REPLAY";
    /** 待支付订单进入关单流程（用户取消或支付超时），开始释放名额 */
    public static final String STAGE_ORDER_CLOSE_REQUESTED = "ORDER_CLOSE_REQUESTED";
    /** 关单完成：MySQL 与 Redis 名额各释放 1，订单转已取消 */
    public static final String STAGE_ORDER_CLOSED = "ORDER_CLOSED";
    /** 关单补偿失败，订单保留 CLOSING 退避重试 */
    public static final String STAGE_ORDER_CLOSE_FAILED = "ORDER_CLOSE_FAILED";

    public static final String LEVEL_INFO = "INFO";
    public static final String LEVEL_WARN = "WARN";
    public static final String LEVEL_ERROR = "ERROR";

    private static final int RETENTION_DAYS = 3;
    private static final int DETAIL_MAX_LENGTH = 480;

    @Resource
    private SeckillMessageEventMapper seckillMessageEventMapper;

    @Value("${seckill.timeline.enabled:true}")
    private boolean enabled;

    public void info(Long orderId, Long userId, Long voucherId, String stage, String detail) {
        record(orderId, userId, voucherId, stage, LEVEL_INFO, detail);
    }

    public void warn(Long orderId, Long userId, Long voucherId, String stage, String detail) {
        record(orderId, userId, voucherId, stage, LEVEL_WARN, detail);
    }

    public void error(Long orderId, Long userId, Long voucherId, String stage, String detail) {
        record(orderId, userId, voucherId, stage, LEVEL_ERROR, detail);
    }

    /** 写一条时间线事件；任何异常都吞掉，只告警，不影响业务 */
    public void record(Long orderId, Long userId, Long voucherId, String stage, String level, String detail) {
        if (!enabled || orderId == null) {
            return;
        }
        try {
            seckillMessageEventMapper.insert(new SeckillMessageEvent()
                    .setOrderId(orderId)
                    .setUserId(userId)
                    .setVoucherId(voucherId)
                    .setStage(stage)
                    .setLevel(level)
                    .setDetail(truncate(detail)));
        } catch (Exception e) {
            log.warn("时间线事件写入失败（不影响业务），orderId={}, stage={}", orderId, stage, e);
        }
    }

    /** 事件表按天清理，避免无限增长（与压测/常态流量相比 3 天足够回看排查） */
    @Scheduled(cron = "0 40 3 * * ?")
    public void cleanup() {
        if (!enabled) {
            return;
        }
        try {
            int deleted = seckillMessageEventMapper.delete(new QueryWrapper<SeckillMessageEvent>()
                    .lt("create_time", LocalDateTime.now().minusDays(RETENTION_DAYS)));
            if (deleted > 0) {
                log.info("秒杀时间线事件清理完成，删除 {} 条（保留 {} 天）", deleted, RETENTION_DAYS);
            }
        } catch (Exception e) {
            log.warn("秒杀时间线事件清理失败: {}", e.getMessage());
        }
    }

    private String truncate(String detail) {
        if (detail == null) {
            return null;
        }
        return detail.length() <= DETAIL_MAX_LENGTH ? detail : detail.substring(0, DETAIL_MAX_LENGTH);
    }
}
