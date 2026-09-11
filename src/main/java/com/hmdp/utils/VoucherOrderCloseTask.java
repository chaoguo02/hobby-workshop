package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.VoucherOrderLifecycleService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

@Component
@Slf4j
public class VoucherOrderCloseTask {

    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    @Resource
    private VoucherOrderLifecycleService lifecycleService;

    @Value("${workshop.order.payment-timeout-seconds:900}")
    private long paymentTimeoutSeconds;

    @Value("${workshop.order.close-batch-size:100}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${workshop.order.close-scan-delay-ms:5000}")
    public void closeExpiredAndRetryCompensation() {
        List<VoucherOrder> expired = voucherOrderMapper.selectList(new QueryWrapper<VoucherOrder>()
                .eq("status", VoucherOrder.STATUS_PENDING_PAYMENT)
                // 扫描和最终 CAS 都使用数据库时间，避免多个应用节点时钟不一致。
                .apply("create_time <= DATE_SUB(NOW(), INTERVAL {0} SECOND)", paymentTimeoutSeconds)
                .orderByAsc("create_time")
                .last("limit " + safeBatchSize()));
        for (VoucherOrder order : expired) {
            try {
                lifecycleService.closeExpiredOrder(order.getId());
            } catch (RuntimeException e) {
                log.error("自动关单失败，orderId={}", order.getId(), e);
            }
        }

        List<VoucherOrder> closing = voucherOrderMapper.selectList(new QueryWrapper<VoucherOrder>()
                .eq("status", VoucherOrder.STATUS_CLOSING)
                .apply("(close_next_retry_time IS NULL OR close_next_retry_time <= NOW())")
                .orderByAsc("close_next_retry_time", "update_time")
                .last("limit " + safeBatchSize()));
        for (VoucherOrder order : closing) {
            try {
                lifecycleService.compensateClosingOrder(order.getId());
            } catch (RuntimeException e) {
                // 单笔异常不能终止本批次，否则后续订单得不到处理。
                log.error("重试关单补偿异常，orderId={}", order.getId(), e);
            }
        }
    }

    private int safeBatchSize() {
        return Math.max(1, Math.min(batchSize, 500));
    }
}
