package com.hmdp.monitor;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.SeckillMessage;
import com.hmdp.mapper.SeckillMessageMapper;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 秒杀链路可观测性：outbox 各状态积压、最老待投递时长、端到端落库延迟、Kafka 消费 lag。
 * 无 dashboard，先以「定时日志 + 受保护端点」两种方式暴露。
 */
@Component
@Slf4j
public class SeckillStatsService {

    private static final String TOPIC = "voucher-orders";
    private static final String GROUP_ID = "voucher-order-group";
    private static final long LATENCY_WINDOW_MINUTES = 5L;

    @Resource
    private SeckillMessageMapper seckillMessageMapper;

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
        return stats;
    }

    /** 定时把快照打进日志，便于没有监控面板时排查 */
    @Scheduled(fixedDelay = 30000)
    public void logSnapshot() {
        log.info("秒杀链路观测: {}", snapshot());
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
}
