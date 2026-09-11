package com.hmdp.monitor;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.SeckillMessage;
import com.hmdp.entity.SeckillMessageEvent;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillMessageEventMapper;
import com.hmdp.mapper.SeckillMessageMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 按订单号还原秒杀链路时间线：把 tb_seckill_message_event 的原始事件
 * 翻译成「阶段中文名 + 相对耗时 + 级别」，供管理后台直接渲染。
 */
@Component
public class SeckillTimelineService {

    /** 阶段码 -> 中文名。新增阶段码时同步在这里加，否则前端显示原始码 */
    private static final Map<String, String> STAGE_NAMES;

    static {
        Map<String, String> names = new HashMap<>();
        names.put(SeckillEventLogger.STAGE_REDIS_ADMIT, "① Redis 预扣库存 / 一人一单校验通过");
        names.put(SeckillEventLogger.STAGE_OUTBOX_WRITTEN, "② 写入本地消息表(outbox)");
        names.put(SeckillEventLogger.STAGE_OUTBOX_WRITE_FAILED, "② 写 outbox 失败（改由恢复任务补单）");
        names.put(SeckillEventLogger.STAGE_RELAY_CLAIMED, "③ Relay 抢占，准备投递 Kafka");
        names.put(SeckillEventLogger.STAGE_KAFKA_SENT, "④ 已投递到 Kafka");
        names.put(SeckillEventLogger.STAGE_RELAY_SEND_FAILED, "④ 投递 Kafka 失败，退避重试");
        names.put(SeckillEventLogger.STAGE_CONSUMER_RECEIVED, "⑤ 消费者收到消息");
        names.put(SeckillEventLogger.STAGE_ORDER_COMMITTED, "⑥ MySQL 写订单 + 扣库存（同事务提交）");
        names.put(SeckillEventLogger.STAGE_CONSUMER_DUPLICATE, "⑤ 重复投递，幂等跳过");
        names.put(SeckillEventLogger.STAGE_CONSUMER_CONFLICT, "⑤ 唯一键冲突且订单不存在（异常）");
        names.put(SeckillEventLogger.STAGE_CONSUMER_FAILED, "⑤ 消费失败，将重试 / 进 DLT");
        names.put(SeckillEventLogger.STAGE_RECOVERY_BACKFILL, "兜底·恢复任务：依 Redis 补 outbox");
        names.put(SeckillEventLogger.STAGE_RECOVERY_CLEANUP, "兜底·恢复任务：回收 Redis 准入记录");
        names.put(SeckillEventLogger.STAGE_RECONCILE_RESET, "兜底·对账任务：卡死重置重投");
        names.put(SeckillEventLogger.STAGE_RECONCILE_FAILED, "兜底·对账任务：重试耗尽转 FAILED");
        names.put(SeckillEventLogger.STAGE_MANUAL_REPLAY, "兜底·人工重放 FAILED→READY");
        names.put(SeckillEventLogger.STAGE_ORDER_CLOSE_REQUESTED, "⑦ 关单受理（用户取消 / 支付超时）");
        names.put(SeckillEventLogger.STAGE_ORDER_CLOSED, "⑧ 关单完成，名额已释放");
        names.put(SeckillEventLogger.STAGE_ORDER_CLOSE_FAILED, "⑧ 关单补偿失败，退避重试中");
        STAGE_NAMES = Collections.unmodifiableMap(names);
    }

    @Resource
    private SeckillMessageMapper seckillMessageMapper;

    @Resource
    private SeckillMessageEventMapper seckillMessageEventMapper;

    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    /** 返回 null 表示该 orderId 没有 outbox 记录 */
    public Map<String, Object> timeline(Long orderId) {
        SeckillMessage message = seckillMessageMapper.selectById(orderId);
        if (message == null) {
            return null;
        }
        List<SeckillMessageEvent> events = seckillMessageEventMapper.selectList(
                new QueryWrapper<SeckillMessageEvent>()
                        .eq("order_id", orderId)
                        .orderByAsc("id"));

        // 订单生命周期状态单独读 tb_voucher_order：outbox 的 COMPLETED 只代表订单落过库，
        // 之后可能被取消/关单，不能拿它当「当前状态」，否则已取消订单会显示「已完成」。
        VoucherOrder order = voucherOrderMapper.selectById(orderId);

        LocalDateTime origin = events.isEmpty() ? message.getCreateTime() : events.get(0).getCreateTime();
        List<Map<String, Object>> items = new ArrayList<>(events.size());
        for (SeckillMessageEvent event : events) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("stage", event.getStage());
            item.put("stageName", STAGE_NAMES.getOrDefault(event.getStage(), event.getStage()));
            item.put("level", event.getLevel());
            item.put("detail", event.getDetail());
            item.put("createTime", event.getCreateTime() == null ? null : event.getCreateTime().toString());
            item.put("offsetMs", offsetMs(origin, event.getCreateTime()));
            items.add(item);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        // orderId 按字符串下发，避免 18 位雪花 ID 在 JS 里丢精度
        result.put("orderId", String.valueOf(message.getOrderId()));
        result.put("userId", message.getUserId());
        result.put("voucherId", message.getVoucherId());
        result.put("status", message.getStatus());
        result.put("statusName", SeckillStatsService.statusName(message.getStatus()));
        result.put("orderStatus", order == null ? null : order.getStatus());
        result.put("orderStatusName", SeckillStatsService.orderStatusName(order == null ? null : order.getStatus()));
        result.put("retry", message.getRetry());
        result.put("createTime", message.getCreateTime() == null ? null : message.getCreateTime().toString());
        result.put("updateTime", message.getUpdateTime() == null ? null : message.getUpdateTime().toString());
        result.put("events", items);
        return result;
    }

    private long offsetMs(LocalDateTime origin, LocalDateTime time) {
        if (origin == null || time == null) {
            return 0L;
        }
        return Duration.between(origin, time).toMillis();
    }
}
