package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
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
import com.hmdp.utils.RedisData;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.constants.RedisConstants.SHOP_GEO_KEY;


@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

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
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 判断是否根据坐标查询
        if(x == null || y == null){
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        // 计算分页参数

        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }

}
