package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Voucher;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherService extends IService<Voucher> {

    Result queryVoucherOfShop(Long shopId);

    void addSeckillVoucher(Voucher voucher);

    /** 全部秒杀券及其 MySQL/Redis 库存、已下单人数，供测试与运维查看 */
    Result listSeckillVouchers();

    /** 直接设置秒杀券库存并同步重置 Redis 库存 key */
    Result updateSeckillStock(Long voucherId, Integer stock);

    /** 清空该券的一人一单集合与准入记录，便于同一用户重复测试 */
    Result resetSeckillOrders(Long voucherId);

    /** 删除秒杀券（tb_voucher + tb_seckill_voucher + Redis 相关 key） */
    Result removeSeckillVoucher(Long voucherId);

}
