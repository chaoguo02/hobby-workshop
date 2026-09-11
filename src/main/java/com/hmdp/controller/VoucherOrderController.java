package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.VoucherOrderLifecycleService;
import com.hmdp.service.impl.VoucherOrderServiceImpl_kafka;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {
    @Resource(name = "voucherOrderServiceImpl_kafka")
    private VoucherOrderServiceImpl_kafka voucherOrderService;

    @Resource
    private VoucherOrderLifecycleService lifecycleService;
    /*
    实现秒杀
     */
    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    /**
     * 查询当前登录用户的预约订单，供“我的预约”和线下核销凭证展示使用。
     */
    @GetMapping("me")
    public Result queryMyOrders() {
        return voucherOrderService.queryMyOrders();
    }

    /**
     * 项目内支付确认入口。真实渠道接入后，应由验签回调触发相同的状态迁移。
     */
    @PostMapping("{id}/pay")
    public Result payOrder(
            @PathVariable("id") Long orderId,
            @RequestParam(value = "payType", defaultValue = "1") Integer payType) {
        return lifecycleService.payMyOrder(orderId, payType);
    }

    /** 用户主动取消尚未支付的预约。 */
    @PostMapping("{id}/cancel")
    public Result cancelOrder(@PathVariable("id") Long orderId) {
        return lifecycleService.cancelMyOrder(orderId);
    }
}
