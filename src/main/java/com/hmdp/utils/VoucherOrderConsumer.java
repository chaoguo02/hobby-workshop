package com.hmdp.utils;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
@Slf4j
public class VoucherOrderConsumer {

    @Resource(name = "voucherOrderServiceImpl_kafka")
    private IVoucherOrderService voucherOrderService;

    @KafkaListener(topics = "voucher-orders", groupId = "voucher-order-group")
    public void handleVoucherOrder(VoucherOrder voucherOrder, Acknowledgment acknowledgment) {
        log.info("收到 Kafka 秒杀订单，orderId={}, userId={}, voucherId={}",
                voucherOrder.getId(), voucherOrder.getUserId(), voucherOrder.getVoucherId());
        if (voucherOrderService.getById(voucherOrder.getId()) != null) {
            log.info("订单已处理，直接确认消息，orderId={}", voucherOrder.getId());
            acknowledgment.acknowledge();
            return;
        }

        try {
            // Service 正常返回时，其内部 MySQL 事务已经提交
            voucherOrderService.createVoucherOrder(voucherOrder);
            acknowledgment.acknowledge();
            log.info("Kafka 秒杀订单已确认，orderId={}", voucherOrder.getId());
        } catch (DuplicateKeyException e) {
            // orderId 主键或 (user_id, voucher_id) 唯一键冲突都视为已处理
            log.info("秒杀订单触发唯一键，跳过重复处理并确认消息，orderId={}, userId={}, voucherId={}",
                    voucherOrder.getId(), voucherOrder.getUserId(), voucherOrder.getVoucherId());
            acknowledgment.acknowledge();
        } catch (RuntimeException e) {
            log.error("处理订单异常，orderId={}", voucherOrder.getId(), e);
            throw e;
        }
    }
}
