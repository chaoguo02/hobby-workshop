package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.entity.SeckillMessage;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillMessageMapper;
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

    @Resource
    private SeckillMessageMapper seckillMessageMapper;

    @KafkaListener(topics = "voucher-orders", groupId = "voucher-order-group")
    public void handleVoucherOrder(VoucherOrder voucherOrder, Acknowledgment acknowledgment) {
        log.info("收到 Kafka 秒杀订单，orderId={}, userId={}, voucherId={}",
                voucherOrder.getId(), voucherOrder.getUserId(), voucherOrder.getVoucherId());

        if (voucherOrderService.getById(voucherOrder.getId()) != null) {
            // 重复投递：订单已在，补齐 COMPLETED 状态（否则会永远停在 SENT）
            markCompleted(voucherOrder.getId());
            log.info("订单已处理，直接确认消息，orderId={}", voucherOrder.getId());
            acknowledgment.acknowledge();
            return;
        }

        try {
            // Service 正常返回时，订单落库、扣库存、置 COMPLETED 已在同一事务提交
            voucherOrderService.createVoucherOrder(voucherOrder);
            acknowledgment.acknowledge();
            log.info("Kafka 秒杀订单已确认，orderId={}", voucherOrder.getId());
        } catch (DuplicateKeyException e) {
            // 唯一键冲突分两种情况，不能一律当成幂等成功
            if (voucherOrderService.getById(voucherOrder.getId()) != null) {
                // case 1：order_id 主键冲突，本订单确实已存在，是真幂等重放
                markCompleted(voucherOrder.getId());
                log.info("秒杀订单重复投递，确认并置 COMPLETED，orderId={}", voucherOrder.getId());
                acknowledgment.acknowledge();
            } else {
                // case 2：(user_id, voucher_id) 唯一键撞车，但本 orderId 并未落库。
                // 当前 orderId 对应的订单并没有成功，不能 ACK，抛出后重试耗尽进入 DLT 人工处理。
                log.error("唯一键冲突但本 orderId 不存在，数据冲突，不能视为成功，orderId={}, userId={}, voucherId={}",
                        voucherOrder.getId(), voucherOrder.getUserId(), voucherOrder.getVoucherId(), e);
                throw e;
            }
        } catch (RuntimeException e) {
            log.error("处理订单异常，orderId={}", voucherOrder.getId(), e);
            throw e;
        }
    }

    private void markCompleted(Long orderId) {
        seckillMessageMapper.update(null,
                new UpdateWrapper<SeckillMessage>()
                        .eq("order_id", orderId)
                        .set("status", SeckillMessage.STATUS_COMPLETED));
    }
}
