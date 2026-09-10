//package com.hmdp.service.impl;
//
//import com.hmdp.entity.VoucherOrder;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.support.Acknowledgment;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.Resource;
//
//@Slf4j
//@Component
//public class VoucherOrderConsumer {
//
//    @Resource
//    private VoucherOrderServiceImpl_kafka voucherorderService;
//
//    @KafkaListener(topics = "voucher-orders",groupId = "voucher-order-group")
//    public void onMessages(VoucherOrder voucherOrder, Acknowledgment ack){
//        try{
//            voucherorderService.createVoucherOrder(voucherOrder);
//            ack.acknowledge();
//        }catch (Exception e){
//            log.error("消费失败，等待重试，orderId={}", voucherOrder.getId(), e);
//            throw e;
//        }
//    }
//}
