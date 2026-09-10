package com.hmdp.utils;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;

@Component
@Slf4j
public class VoucherOrderConsumer {

    @Resource(name = "voucherOrderServiceImpl_kafka")
    private IVoucherOrderService voucherOrderService;

    @Resource
    private RedissonClient redissonClient;

    @Transactional(rollbackFor = Exception.class)
    @KafkaListener(topics = "voucher-orders", groupId = "voucher-order-group")
    public void handleVoucherOrder(VoucherOrder voucherOrder, Acknowledgment acknowledgment) {
        if (voucherOrderService.getById(voucherOrder.getId()) != null) {
            log.info("订单已处理，直接确认消息，orderId={}", voucherOrder.getId());
            acknowledgment.acknowledge();
            return;
        }

        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            throw new IllegalStateException("用户订单正在处理中，userId=" + userId);
        }

        try {
            voucherOrderService.createVoucherOrder(voucherOrder);
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    acknowledgment.acknowledge();
                }
            });
        } catch (RuntimeException e) {
            log.error("处理订单异常，orderId={}", voucherOrder.getId(), e);
            throw e;
        } finally {
            lock.unlock();
        }
    }
}
