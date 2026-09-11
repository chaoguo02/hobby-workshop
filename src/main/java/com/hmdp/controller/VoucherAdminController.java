package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.entity.Voucher;
import com.hmdp.service.IVoucherService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * 秒杀券管理端点，用于本地压测/联调时自助造数与清理。
 * 与其它 /monitor/** 一样，需要同时携带登录头 authorization 与管理员令牌头 X-Monitor-Token。
 * 注意：这是管理能力，不属于业务接口，生产环境应下线或限制来源。
 */
@RestController
@RequestMapping("/monitor/voucher")
public class VoucherAdminController {

    @Resource
    private IVoucherService voucherService;

    /** 全部秒杀券：MySQL 库存、Redis 库存、已下单人数 */
    @GetMapping
    public Result list() {
        return voucherService.listSeckillVouchers();
    }

    /**
     * 新建秒杀券，body 与 POST /voucher/seckill 一致。
     * 必填 stock；type 建议传 2（秒杀券）；beginTime/endTime 决定 Redis 库存 key 的 TTL。
     */
    @PostMapping
    public Result create(@RequestBody Voucher voucher) {
        if (voucher.getStock() == null || voucher.getStock() < 0) {
            return Result.fail("stock 必填且为非负整数");
        }
        voucherService.addSeckillVoucher(voucher);
        return Result.ok(voucher.getId());
    }

    /** 直接设置库存，同时重置 Redis 库存 key（Redis 是准入真值） */
    @PutMapping("/{id}/stock")
    public Result updateStock(@PathVariable("id") Long id, @RequestParam("stock") Integer stock) {
        return voucherService.updateSeckillStock(id, stock);
    }

    /** 清空该券的一人一单集合与准入记录，让同一用户可重复下单测试 */
    @PostMapping("/{id}/reset-orders")
    public Result resetOrders(@PathVariable("id") Long id) {
        return voucherService.resetSeckillOrders(id);
    }

    /** 删除秒杀券及 Redis 的库存/一人一单/准入 key */
    @DeleteMapping("/{id}")
    public Result delete(@PathVariable("id") Long id) {
        return voucherService.removeSeckillVoucher(id);
    }
}
