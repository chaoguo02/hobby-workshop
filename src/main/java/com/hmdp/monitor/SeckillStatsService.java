package com.hmdp.monitor;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.SeckillMessage;
import com.hmdp.entity.SeckillMessageEvent;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillMessageEventMapper;
import com.hmdp.mapper.SeckillMessageMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 秒杀链路可观测性：outbox 各状态积压、最老待投递时长、端到端落库延迟、Kafka 消费 lag。
 * 无 dashboard，先以「定时日志 + 受保护端点」两种方式暴露。
 */
@Component
@Slf4j
public class SeckillStatsService {

    private static final String TOPIC = "voucher-orders";
    private static final String DLT_TOPIC = TOPIC + ".DLT";
    private static final String GROUP_ID = "voucher-order-group";
    private static final long LATENCY_WINDOW_MINUTES = 5L;

    @Resource
    private SeckillMessageMapper seckillMessageMapper;

    @Resource
    private SeckillMessageEventMapper seckillMessageEventMapper;

    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    @Value("${workshop.order.payment-timeout-seconds:900}")
    private long paymentTimeoutSeconds;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private AdminClient adminClient;

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        adminClient = AdminClient.create(props);
    }

    @PreDestroy
    public void destroy() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> stats = new LinkedHashMap<>();
        Map<String, Object> counts = outboxStatusCounts();
        stats.put("outboxStatusCounts", counts);
        stats.put("failedCount", counts.getOrDefault(String.valueOf(SeckillMessage.STATUS_FAILED), 0));
        stats.put("oldestReadyAgeSeconds", oldestReadyAgeSeconds());
        stats.put("oldestFailedAgeSeconds", oldestFailedAgeSeconds());
        stats.put("completedLatencyLast" + LATENCY_WINDOW_MINUTES + "m", completedLatency());
        stats.put("kafkaLag", kafkaLag());
        stats.put("dltBacklog", dltBacklog());
        stats.put("orderClose", orderCloseStats());
        return stats;
    }

    /** 定时把快照打进日志，便于没有监控面板时排查 */
    @Scheduled(fixedDelay = 30000)
    public void logSnapshot() {
        Map<String, Object> stats = snapshot();
        log.info("秒杀链路观测: {}", stats);
        alertOrderClose(stats);
    }

    /**
     * 关单补偿失败会一直停在 CLOSING 无限退避重试，面板上不醒目，
     * 按 30s 节奏主动打 ERROR，提醒人工去看 close_last_error，与 outbox FAILED 同级别。
     */
    @SuppressWarnings("unchecked")
    private void alertOrderClose(Map<String, Object> stats) {
        Object close = stats.get("orderClose");
        if (!(close instanceof Map)) {
            return;
        }
        long retrying = num(((Map<String, Object>) close).get("retrying"));
        if (retrying > 0) {
            log.error("关单补偿积压告警：{} 笔订单停在 CLOSING 反复重试（最多 {} 次），请检查 close_last_error 与 Redis 连通性",
                    retrying, num(((Map<String, Object>) close).get("maxRetry")));
        }
    }

    /**
     * 面向管理后台的「一眼看懂」视图：把 outbox 状态码翻译成带中文的阶段漏斗，
     * 外加近期每一笔订单所处阶段、端到端耗时与 Kafka 积压。
     * 所有数字都来自 outbox 一张表，不额外读 Redis，刷新代价小。
     */
    public Map<String, Object> pipeline() {
        Map<String, Object> counts = outboxStatusCounts();
        long ready = num(counts.get(String.valueOf(SeckillMessage.STATUS_READY)));
        long processing = num(counts.get(String.valueOf(SeckillMessage.STATUS_PROCESSING)));
        long sent = num(counts.get(String.valueOf(SeckillMessage.STATUS_SENT)));
        long completed = num(counts.get(String.valueOf(SeckillMessage.STATUS_COMPLETED)));
        long failed = num(counts.get(String.valueOf(SeckillMessage.STATUS_FAILED)));
        long total = ready + processing + sent + completed + failed;

        List<Map<String, Object>> stages = new ArrayList<>();
        stages.add(stage("accepted", "抢购成功", "Redis 原子扣库存 + 一人一单通过，已拿到资格", total,
                total > 0 ? "done" : "idle"));
        stages.add(stage("ready", "排队待投递", "已写入 outbox，等 relay 投递 Kafka", ready,
                ready > 0 ? "active" : "idle"));
        stages.add(stage("delivering", "投递 / 消费中", "已发往 Kafka，消费者正在生成订单", processing + sent,
                (processing + sent) > 0 ? "active" : "idle"));
        stages.add(stage("completed", "订单已落库", "订单写入 MySQL 且库存已扣减，链路终点", completed, "done"));
        if (failed > 0) {
            stages.add(stage("failed", "异常待人工", "重试耗尽，需在下方点『重放』", failed, "danger"));
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("stages", stages);
        result.put("total", total);
        result.put("inFlight", ready + processing + sent);
        result.put("failed", failed);
        result.put("oldestReadyAgeSeconds", oldestReadyAgeSeconds());
        result.put("oldestFailedAgeSeconds", oldestFailedAgeSeconds());
        result.put("completedLatencyLast5m", completedLatency());
        result.put("kafkaLag", kafkaLag());
        result.put("dltBacklog", dltBacklog());
        result.put("orderClose", orderCloseStats());
        result.put("recentOrders", recentOrders());
        return result;
    }

    private Map<String, Object> orderCloseStats() {
        List<Map<String, Object>> rows = voucherOrderMapper.selectMaps(
                new QueryWrapper<VoucherOrder>().select(
                        "SUM(CASE WHEN status = 1 AND create_time <= DATE_SUB(NOW(), INTERVAL "
                                + paymentTimeoutSeconds + " SECOND) THEN 1 ELSE 0 END) AS expired_pending",
                        "SUM(CASE WHEN status = 7 THEN 1 ELSE 0 END) AS closing",
                        "SUM(CASE WHEN status = 7 AND close_retry > 0 THEN 1 ELSE 0 END) AS retrying",
                        "MAX(CASE WHEN status = 7 THEN close_retry ELSE 0 END) AS max_retry"));
        Map<String, Object> result = new LinkedHashMap<>();
        Map<String, Object> row = rows.isEmpty() || rows.get(0) == null
                ? Collections.emptyMap() : rows.get(0);
        result.put("expiredPending", num(row.get("expired_pending")));
        result.put("closing", num(row.get("closing")));
        result.put("retrying", num(row.get("retrying")));
        result.put("maxRetry", num(row.get("max_retry")));
        return result;
    }

    private List<Map<String, Object>> recentOrders() {
        // 见下方 attachWarnCount：warnCount 取时间线里 WARN/ERROR 事件数，
        // 比 outbox.retry 更可靠——人工重放会把 retry 归零，但事件历史不会丢。
        List<Map<String, Object>> rows = seckillMessageMapper.selectMaps(
                new QueryWrapper<SeckillMessage>()
                        .select("order_id", "voucher_id", "user_id", "status", "retry",
                                // 终态看端到端耗时，在途看已等待时长，两者语义不同但都是"这一笔花了多久"
                                "CASE WHEN status IN (3, 4) THEN TIMESTAMPDIFF(MICROSECOND, create_time, update_time)"
                                + " ELSE TIMESTAMPDIFF(MICROSECOND, create_time, NOW()) END / 1000 AS elapsed_ms")
                        .orderByDesc("create_time")
                        .last("LIMIT 20"));
        List<Map<String, Object>> list = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            if (row == null) {
                continue;
            }
            Map<String, Object> item = new LinkedHashMap<>();
            // 雪花 orderId 是 18 位，超出 JS Number.MAX_SAFE_INTEGER，必须按字符串下发，否则前端丢精度、按 orderId 查时间线会查错
            item.put("orderId", String.valueOf(row.get("order_id")));
            item.put("voucherId", row.get("voucher_id"));
            item.put("userId", row.get("user_id"));
            item.put("status", row.get("status"));
            item.put("statusName", statusName((int) num(row.get("status"))));
            item.put("retry", row.get("retry"));
            item.put("elapsedMs", row.get("elapsed_ms"));
            item.put("warnCount", 0L);
            list.add(item);
        }
        attachWarnCount(list);
        attachOrderStatus(list);
        return list;
    }

    /**
     * 给最近订单补上真正的订单生命周期状态（tb_voucher_order）。
     * outbox 停在 COMPLETED 只说明订单落过库，之后可能已被取消/关单，
     * 两者混用会让已取消订单在后台显示「已完成」。
     */
    private void attachOrderStatus(List<Map<String, Object>> list) {
        if (list.isEmpty()) {
            return;
        }
        List<Long> orderIds = new ArrayList<>(list.size());
        for (Map<String, Object> item : list) {
            orderIds.add(Long.valueOf((String) item.get("orderId")));
        }
        Map<Long, Integer> statusByOrder = new HashMap<>();
        for (VoucherOrder order : voucherOrderMapper.selectBatchIds(orderIds)) {
            statusByOrder.put(order.getId(), order.getStatus());
        }
        for (Map<String, Object> item : list) {
            Integer orderStatus = statusByOrder.get(Long.valueOf((String) item.get("orderId")));
            item.put("orderStatus", orderStatus);
            item.put("orderStatusName", orderStatusName(orderStatus));
        }
    }

    /**
     * 给最近订单补上「时间线里出现过多少条 WARN/ERROR 事件」。
     * 用它而不是 outbox.retry 判断「有没有重试过」：人工重放会把 retry 归零，
     * 但事件历史是 append-only 的，只要重试过就一定留痕。
     */
    private void attachWarnCount(List<Map<String, Object>> list) {
        if (list.isEmpty()) {
            return;
        }
        List<Long> orderIds = new ArrayList<>(list.size());
        for (Map<String, Object> item : list) {
            orderIds.add(Long.valueOf((String) item.get("orderId")));
        }
        Map<Long, Long> warnByOrder = new HashMap<>();
        List<Map<String, Object>> rows = seckillMessageEventMapper.selectMaps(
                new QueryWrapper<SeckillMessageEvent>()
                        .select("order_id",
                                "SUM(CASE WHEN level IN ('WARN', 'ERROR') THEN 1 ELSE 0 END) AS warn_cnt")
                        .in("order_id", orderIds)
                        .groupBy("order_id"));
        for (Map<String, Object> row : rows) {
            if (row == null) {
                continue;
            }
            warnByOrder.put(num(row.get("order_id")), num(row.get("warn_cnt")));
        }
        for (Map<String, Object> item : list) {
            item.put("warnCount", warnByOrder.getOrDefault(Long.valueOf((String) item.get("orderId")), 0L));
        }
    }

    public static String statusName(Integer status) {
        if (status == null) {
            return "未知";
        }
        switch (status) {
            case SeckillMessage.STATUS_READY: return "排队待投递";
            case SeckillMessage.STATUS_PROCESSING: return "投递中";
            case SeckillMessage.STATUS_SENT: return "已进 Kafka";
            case SeckillMessage.STATUS_COMPLETED: return "已完成";
            case SeckillMessage.STATUS_FAILED: return "异常待人工";
            default: return "未知";
        }
    }

    /**
     * 订单生命周期状态（tb_voucher_order）的展示名，必须和上面的投递状态（outbox）分开：
     * outbox 的 COMPLETED 只代表订单已落库，订单之后仍可能被取消/关单。
     */
    public static String orderStatusName(Integer status) {
        if (status == null) {
            return "订单未创建";
        }
        switch (status) {
            case VoucherOrder.STATUS_PENDING_PAYMENT: return "待支付";
            case VoucherOrder.STATUS_PENDING_REDEMPTION: return "待核销";
            case VoucherOrder.STATUS_REDEEMED: return "已核销";
            case VoucherOrder.STATUS_CANCELLED: return "已取消";
            case VoucherOrder.STATUS_CLOSING: return "关单中";
            default: return "未知";
        }
    }

    private Map<String, Object> stage(String key, String name, String desc, long count, String tone) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("key", key);
        node.put("name", name);
        node.put("desc", desc);
        node.put("count", count);
        node.put("tone", tone);
        return node;
    }

    private long num(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private Map<String, Object> outboxStatusCounts() {
        Map<String, Object> counts = new LinkedHashMap<>();
        for (Map<String, Object> row : seckillMessageMapper.selectMaps(
                new QueryWrapper<SeckillMessage>()
                        .select("status", "COUNT(*) AS cnt")
                        .groupBy("status"))) {
            counts.put(String.valueOf(row.get("status")), row.get("cnt"));
        }
        return counts;
    }

    private Object oldestReadyAgeSeconds() {
        List<Map<String, Object>> rows = seckillMessageMapper.selectMaps(
                new QueryWrapper<SeckillMessage>()
                        .select("TIMESTAMPDIFF(SECOND, MIN(create_time), NOW()) AS oldest_age")
                        .eq("status", SeckillMessage.STATUS_READY));
        // 无 READY 时聚合行全为 NULL，MyBatis 会返回一个 null 元素，需判空
        if (rows.isEmpty() || rows.get(0) == null) {
            return null;
        }
        return rows.get(0).get("oldest_age");
    }

    private Object oldestFailedAgeSeconds() {
        List<Map<String, Object>> rows = seckillMessageMapper.selectMaps(
                new QueryWrapper<SeckillMessage>()
                        .select("TIMESTAMPDIFF(SECOND, MIN(update_time), NOW()) AS oldest_age")
                        .eq("status", SeckillMessage.STATUS_FAILED));
        if (rows.isEmpty() || rows.get(0) == null) {
            return null;
        }
        return rows.get(0).get("oldest_age");
    }

    private Map<String, Object> completedLatency() {
        List<Map<String, Object>> rows = seckillMessageMapper.selectMaps(
                new QueryWrapper<SeckillMessage>()
                        .select("COUNT(*) AS cnt",
                                "AVG(TIMESTAMPDIFF(MICROSECOND, create_time, update_time)) / 1000 AS avg_ms",
                                "MAX(TIMESTAMPDIFF(MICROSECOND, create_time, update_time)) / 1000 AS max_ms")
                        .eq("status", SeckillMessage.STATUS_COMPLETED)
                        .apply("update_time >= DATE_SUB(NOW(), INTERVAL {0} MINUTE)", LATENCY_WINDOW_MINUTES));
        Map<String, Object> result = new LinkedHashMap<>();
        if (!rows.isEmpty() && rows.get(0) != null) {
            Map<String, Object> row = rows.get(0);
            result.put("count", row.get("cnt"));
            result.put("avgMs", row.get("avg_ms"));
            result.put("maxMs", row.get("max_ms"));
        }
        return result;
    }

    private Map<String, Object> kafkaLag() {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            TopicDescription description = adminClient
                    .describeTopics(Collections.singletonList(TOPIC))
                    .all().get(5, TimeUnit.SECONDS).get(TOPIC);
            List<TopicPartition> partitions = new ArrayList<>();
            for (TopicPartitionInfo info : description.partitions()) {
                partitions.add(new TopicPartition(TOPIC, info.partition()));
            }
            Map<TopicPartition, OffsetAndMetadata> committed = adminClient
                    .listConsumerGroupOffsets(GROUP_ID)
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetSpec> offsetRequest = new LinkedHashMap<>();
            for (TopicPartition tp : partitions) {
                offsetRequest.put(tp, OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endInfos = adminClient
                    .listOffsets(offsetRequest).all().get(5, TimeUnit.SECONDS);

            long total = 0L;
            Map<String, Object> perPartition = new LinkedHashMap<>();
            for (TopicPartition tp : partitions) {
                long end = endInfos.containsKey(tp) ? endInfos.get(tp).offset() : 0L;
                OffsetAndMetadata meta = committed.get(tp);
                long offset = meta == null ? 0L : meta.offset();
                long lag = Math.max(0L, end - offset);
                total += lag;
                perPartition.put("p" + tp.partition(), lag);
            }
            result.put("totalLag", total);
            result.put("partitions", perPartition);
        } catch (Exception e) {
            log.warn("获取 Kafka consumer lag 失败: {}", e.getMessage());
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * DLT 是「只进不出」的隔离桶：没有消费者，所以堆积量 = 各分区 (log-end-offset - log-start-offset) 之和。
     * 这里不用 consumer lag 是因为没有消费组；用 earliest/latest 之差还能自然抵消 retention 删除的旧段。
     * 正常情况下应恒为 0；一旦非 0，说明有订单在消费者侧重试 4 次后仍失败被隔离，
     * 且 outbox 那行会停在 SENT、随后被 ReconcileTask 顶成 FAILED 走人工重放——所以这里只做「有人掉队了」的告警。
     */
    private Map<String, Object> dltBacklog() {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            // 先查存在性再 describe：describeTopics 在 broker 开启 auto.create.topics.enable 时会顺手建 topic，
            // 那样建出来的 DLT 只有 1 个分区，会让按源分区号写 DLT 的 recoverer 直接失败，绝不能在这里触发。
            Set<String> topics = adminClient.listTopics().names().get(5, TimeUnit.SECONDS);
            if (!topics.contains(DLT_TOPIC)) {
                result.put("count", 0L);
                result.put("exists", false);
                return result;
            }
            TopicDescription description = adminClient
                    .describeTopics(Collections.singletonList(DLT_TOPIC))
                    .all().get(5, TimeUnit.SECONDS).get(DLT_TOPIC);
            Map<TopicPartition, OffsetSpec> earliestRequest = new LinkedHashMap<>();
            Map<TopicPartition, OffsetSpec> latestRequest = new LinkedHashMap<>();
            for (TopicPartitionInfo info : description.partitions()) {
                TopicPartition tp = new TopicPartition(DLT_TOPIC, info.partition());
                earliestRequest.put(tp, OffsetSpec.earliest());
                latestRequest.put(tp, OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest = adminClient
                    .listOffsets(earliestRequest).all().get(5, TimeUnit.SECONDS);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = adminClient
                    .listOffsets(latestRequest).all().get(5, TimeUnit.SECONDS);

            long total = 0L;
            Map<String, Object> perPartition = new LinkedHashMap<>();
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : latest.entrySet()) {
                TopicPartition tp = entry.getKey();
                long start = earliest.containsKey(tp) ? earliest.get(tp).offset() : 0L;
                long count = Math.max(0L, entry.getValue().offset() - start);
                total += count;
                perPartition.put("p" + tp.partition(), count);
            }
            result.put("count", total);
            result.put("exists", true);
            result.put("partitions", perPartition);
        } catch (Exception e) {
            log.warn("获取 DLT 堆积量失败: {}", e.getMessage());
            result.put("error", e.getMessage());
        }
        return result;
    }
}
