package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.constants.RedisConstants;
import com.hmdp.constants.SystemConstants;
import com.hmdp.dto.Result;
import com.hmdp.entity.CacheInvalidationEvent;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.CacheInvalidationEventMapper;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.CacheInvalidationRelay;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;


@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private CacheClient cacheClient;

    @Resource
    private CacheInvalidationEventMapper cacheInvalidationEventMapper;

    @Resource
    private CacheInvalidationRelay cacheInvalidationRelay;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
//         Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES,
//                 RedisConstants.CACHE_SHOP_KEY,id,Shop.class,this::getById);

        // 互斥锁解决缓存击穿
//        Shop shop = cacheClient.queryWithMutex(RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES,
//                 RedisConstants.CACHE_SHOP_KEY,id,Shop.class,this::getById);



//         逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES,
                RedisConstants.CACHE_SHOP_KEY,id,Shop.class,this::getById);


        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库：检查影响行数，更新失败（id 不存在/并发覆盖）不能返回成功
        if (!updateById(shop)) {
            return Result.fail("店铺更新失败");
        }
        // 2.失效事件与业务更新同事务落库；写入失败则回滚业务更新，避免产生不可恢复的脏缓存。
        CacheInvalidationEvent event = new CacheInvalidationEvent()
                .setCacheKey(RedisConstants.CACHE_SHOP_KEY + id)
                .setLockKey(RedisConstants.LOCK_SHOP_KEY + id)
                .setStatus(CacheInvalidationEvent.STATUS_PENDING)
                .setRetry(0);
        if (cacheInvalidationEventMapper.insert(event) != 1) {
            throw new IllegalStateException("记录缓存失效事件失败，shopId=" + id);
        }
        // 3.提交成功后立即尝试；Redis 故障时数据库事件仍在，定时任务会继续重试。
        deleteCacheAfterCommit(event.getId());
        return Result.ok();

    }

    /**
     * 事务提交后再失效缓存（L1 + L2）。若在事务内删除，并发读可能在事务提交前读到旧库值并回填缓存，
     * 而删除已经发生，脏数据会一直留到缓存过期。
     * 无事务环境（如直接调用）则立即失效。
     */
    private void deleteCacheAfterCommit(Long eventId) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
                @Override
                public void afterCommit() {
                    cacheInvalidationRelay.deliverNow(eventId);
                }
            });
        } else {
            cacheInvalidationRelay.deliverNow(eventId);
        }
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current) {
        Page<Shop> page = query()
                .eq("type_id", typeId)
                .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
        return Result.ok(page.getRecords());
    }

}
