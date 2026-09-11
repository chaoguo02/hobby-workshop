package com.hmdp.utils;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CacheClientTest {

    private CacheClient cacheClient;
    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOperations;
    private RedissonClient redissonClient;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        cacheClient = new CacheClient();
        redisTemplate = mock(StringRedisTemplate.class);
        valueOperations = mock(ValueOperations.class);
        redissonClient = mock(RedissonClient.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        ReflectionTestUtils.setField(cacheClient, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(cacheClient, "redissonClient", redissonClient);
    }

    @Test
    void redisHitShouldFillLocalCache() {
        Shop shop = new Shop();
        shop.setId(1L);
        shop.setName("new-name");
        when(valueOperations.get("cache:shop:1")).thenReturn(JSONUtil.toJsonStr(shop));

        Shop first = cacheClient.queryWithPassThrough(30L, TimeUnit.MINUTES,
                "cache:shop:", 1L, Shop.class, id -> null);
        Shop second = cacheClient.queryWithPassThrough(30L, TimeUnit.MINUTES,
                "cache:shop:", 1L, Shop.class, id -> null);

        assertEquals("new-name", first.getName());
        assertSame(first, second);
        verify(valueOperations).get("cache:shop:1");
    }

    @Test
    void emptyRedisValueShouldBeCachedLocally() {
        AtomicInteger databaseCalls = new AtomicInteger();
        when(valueOperations.get("cache:shop:404")).thenReturn("");

        Shop first = cacheClient.queryWithPassThrough(30L, TimeUnit.MINUTES,
                "cache:shop:", 404L, Shop.class, id -> {
                    databaseCalls.incrementAndGet();
                    return null;
                });
        Shop second = cacheClient.queryWithPassThrough(30L, TimeUnit.MINUTES,
                "cache:shop:", 404L, Shop.class, id -> null);

        assertNull(first);
        assertNull(second);
        assertEquals(0, databaseCalls.get());
        verify(valueOperations).get("cache:shop:404");
    }

    @Test
    void coldLogicalCacheShouldReturnLoadedValue() {
        RLock lock = mock(RLock.class);
        when(redissonClient.getLock("lock:shop:2")).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        when(valueOperations.get("cache:shop:2")).thenReturn(null, null);
        Shop databaseShop = new Shop();
        databaseShop.setId(2L);

        Shop result = cacheClient.queryWithLogicalExpire(30L, TimeUnit.MINUTES,
                "cache:shop:", 2L, Shop.class, id -> databaseShop);

        assertSame(databaseShop, result);
        verify(lock).unlock();
        verify(valueOperations).set(eq("cache:shop:2"), anyString(), anyLong(), eq(TimeUnit.SECONDS));
    }

    @Test
    void redisFailureShouldUseLimitedDatabaseFallbackAndThenL1() {
        when(valueOperations.get("cache:shop:3"))
                .thenThrow(new RedisConnectionFailureException("redis down"));
        AtomicInteger databaseCalls = new AtomicInteger();
        Shop databaseShop = new Shop();
        databaseShop.setId(3L);

        Shop first = cacheClient.queryWithPassThrough(30L, TimeUnit.MINUTES,
                "cache:shop:", 3L, Shop.class, id -> {
                    databaseCalls.incrementAndGet();
                    return databaseShop;
                });
        Shop second = cacheClient.queryWithPassThrough(30L, TimeUnit.MINUTES,
                "cache:shop:", 3L, Shop.class, id -> null);

        assertSame(databaseShop, first);
        assertSame(first, second);
        assertEquals(1, databaseCalls.get());
        verify(valueOperations).get("cache:shop:3");
        verify(valueOperations, never()).set(eq("cache:shop:3"), anyString(), anyLong(), eq(TimeUnit.SECONDS));
    }

    @Test
    void expiredHotKeyShouldOnlyScheduleOneLocalRebuild() throws Exception {
        RedisData expired = new RedisData();
        Shop oldShop = new Shop();
        oldShop.setId(4L);
        oldShop.setName("old");
        expired.setData(oldShop);
        expired.setExpireTime(java.time.LocalDateTime.now().minusSeconds(1));
        when(valueOperations.get("cache:shop:4")).thenReturn(JSONUtil.toJsonStr(expired));

        RLock lock = mock(RLock.class);
        when(redissonClient.getLock("lock:shop:4")).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);

        AtomicInteger databaseCalls = new AtomicInteger();
        CountDownLatch rebuildStarted = new CountDownLatch(1);
        CountDownLatch allowRebuild = new CountDownLatch(1);
        CountDownLatch rebuildFinished = new CountDownLatch(1);
        Shop freshShop = new Shop();
        freshShop.setId(4L);
        freshShop.setName("fresh");

        cacheClient.queryWithLogicalExpire(30L, TimeUnit.MINUTES,
                "cache:shop:", 4L, Shop.class, id -> {
                    databaseCalls.incrementAndGet();
                    rebuildStarted.countDown();
                    try {
                        allowRebuild.await(2, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        rebuildFinished.countDown();
                    }
                    return freshShop;
                });
        rebuildStarted.await(2, TimeUnit.SECONDS);

        for (int i = 0; i < 20; i++) {
            cacheClient.queryWithLogicalExpire(30L, TimeUnit.MINUTES,
                    "cache:shop:", 4L, Shop.class, id -> freshShop);
        }
        allowRebuild.countDown();
        rebuildFinished.await(2, TimeUnit.SECONDS);

        assertEquals(1, databaseCalls.get());
    }

    @Test
    void invalidationBroadcastShouldRemoveLocalValue() {
        Shop oldShop = new Shop();
        oldShop.setId(5L);
        oldShop.setName("old");
        cacheClient.set("cache:shop:5", oldShop, 30L, TimeUnit.MINUTES);

        CacheInvalidationSubscriber subscriber = new CacheInvalidationSubscriber();
        ReflectionTestUtils.setField(subscriber, "cacheClient", cacheClient);
        Message message = mock(Message.class);
        when(message.getBody()).thenReturn("cache:shop:5".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        subscriber.onMessage(message, null);

        Shop freshShop = new Shop();
        freshShop.setId(5L);
        freshShop.setName("fresh");
        when(valueOperations.get("cache:shop:5")).thenReturn(JSONUtil.toJsonStr(freshShop));
        Shop result = cacheClient.queryWithPassThrough(30L, TimeUnit.MINUTES,
                "cache:shop:", 5L, Shop.class, id -> null);

        assertEquals("fresh", result.getName());
    }
}
