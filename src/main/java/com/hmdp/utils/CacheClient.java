package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.constants.RedisConstants;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class CacheClient {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void set(String key, Object value, Long time, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,timeUnit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit timeUnit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(Long time, TimeUnit timeUnit,String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback){
        // 1. 从redis查询商铺缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. 缓存是否存在
        if(StrUtil.isNotBlank(json)){
            // 3. 存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        // 4. 不存在，根据id查询数据库
        if(json != null){
            return null;
        }

        R r = dbFallback.apply(id);
        // 5. 不存在返回错误
        if(r == null){
            stringRedisTemplate.opsForValue().set(key, "",
                    RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        // 6. 存在，写入redis
        this.set(key, r, time, timeUnit);

        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R, ID> R queryWithLogicalExpire(Long time, TimeUnit timeUnit,String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback){
        // 1. 从redis查询商铺缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. 缓存是否存在
        // 应该先去判断一下缓存的是空值，还是没有这个缓存
        // 如果是没有这个缓存空值的话直接返回null，否则都应该去重建缓存

        if(StrUtil.isBlank(json)){
            // 如果存在说明是缓存的空值
            if(Boolean.TRUE.equals(stringRedisTemplate.hasKey(key)))
                return null;
            // 如果不存在说明是这是第一次查询，所以要构建一下缓存
            else{
                // 构建缓存
                R r1 = dbFallback.apply(id);
                this.setWithLogicalExpire(key, r1, time,timeUnit);
                return null;
            }
        }

        // 如果没有的话，新建缓存


        /** 命中，需要先把json反序列化为对象
         *  判断是否过期
         *  未过期，直接返回店铺信息
         *  已过期，需要缓存重建
         *  缓存重建
         *  获取互斥锁
         *  判断是否获取锁成功
         *  成功，开启独立线程，实现缓存重建
         *  返回过期的商铺信息
         */
        // 3.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 4.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now()) ){
            // 4.1 未过期，直接返回店铺信息
            return r;
        }
        // 4.2 已过期，需要缓存重建
        // 缓存重建
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        // 5 获取互斥锁
        boolean isLock = tryLock(lockKey);
        if(isLock){
            log.debug("获取到互斥锁，开始重建缓存");
            // 获取互斥锁成功，开启独立线程，实现缓存重建，获取成功后，再简称为redis缓存是否过期，做DoubleCheck
            // 如果DoubleCheck存在，则无需重建缓存
            CACHE_REBUILD_EXECUTOR.submit(
                    ()->{
                        try {
                            R r1 = dbFallback.apply(id);
                            this.setWithLogicalExpire(key, r1, time,timeUnit);
                        }
                        catch (Exception e){
                            throw new RuntimeException(e);
                        }
                        finally {
                            unlock(lockKey);
                        }
                    }
            );
        }

        return r;
    }
    public <R, ID> R queryWithMutex(Long time, TimeUnit timeUnit,String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback){
        // 1. 从redis查询商铺缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. 缓存是否存在
        if(StrUtil.isNotBlank(json)){
            // 3. 存在，直接返回
            R r = JSONUtil.toBean(json, type);
            return r;
        }
        // 4. 不存在，根据id查询数据库
        if(json != null){
            return null;
        }
        // 实现缓存重建
        /**
         * 获取互斥锁
         * 判断是否获取成功
         * 失败则休眠并重试
         * 成功，根据id查询
         */
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);
            if(!isLock){
                Thread.sleep(50);
                return queryWithMutex(time, timeUnit, keyPrefix, id, type, dbFallback);
            }
            // 根据id查询到数据库数据
            r = dbFallback.apply(id);
            // 5. 不存在返回错误
            if(r == null){
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "",
                        RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            // 6. 存在，写入redis
            this.set(key, r, time, timeUnit);
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }
        finally {
            // 释放互斥锁
            unlock(lockKey);
        }
        return r;
    }


    /**
     * 获取和释放锁
     */
    private boolean tryLock(String key){
        // set key value nx ex 加锁
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }


}
