package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.hmdp.constants.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
@Slf4j
public class CacheClient {

    /** 重建任务被拒绝（队列满）与重建失败的计数，便于排查"缓存一直不更新" */
    private static final LongAdder REBUILD_REJECTED = new LongAdder();
    private static final LongAdder REBUILD_FAILED = new LongAdder();
    private static final LongAdder REBUILD_SUCCEEDED = new LongAdder();
    private static final LongAdder REDIS_HITS = new LongAdder();
    private static final LongAdder REDIS_MISSES = new LongAdder();
    private static final LongAdder REDIS_FAILURES = new LongAdder();
    private static final LongAdder NULL_HITS = new LongAdder();
    private static final LongAdder STALE_RETURNS = new LongAdder();
    private static final LongAdder DB_LOADS = new LongAdder();

    /** Redis 故障时最多允许 32 个请求同时回源数据库。 */
    private static final Semaphore DB_DEGRADE_GUARD = new Semaphore(32);

    private static final int REBUILD_QUEUE_CAPACITY = 256;

    /**
     * 有界重建线程池：无界队列会在数据库故障时无限堆积任务。
     * 队列满时直接丢弃并计数告警（调用方本就返回旧值，不需要阻塞 Tomcat 线程），
     * 线程设为 daemon，避免应用停止时被重建任务卡住 JVM 退出。
     */
    private static final ThreadPoolExecutor CACHE_REBUILD_EXECUTOR = new ThreadPoolExecutor(
            4, 8, 60L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(REBUILD_QUEUE_CAPACITY),
            new NamedThreadFactory("cache-rebuild-"),
            new ThreadPoolExecutor.AbortPolicy());

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    /** 防止同一 JVM 为同一个过期 key 重复堆积重建任务；跨实例互斥由 Redisson 负责。 */
    private final Set<String> rebuildingKeys = ConcurrentHashMap.newKeySet();

    /**
     * L1 本地缓存。短 TTL 是它的关键约束：多实例失效广播尚未接入时，
     * 它也是"别的实例改了库、本实例 L1 仍持有旧值"这一脏读窗口的唯一上界。
     */
    private static final long L1_MAX_SIZE = 10_000L;
    private static final long L1_TTL_SECONDS = 30L;

    private final Cache<String, LocalValue> localCache = Caffeine.newBuilder()
            .maximumSize(L1_MAX_SIZE)
            .expireAfterWrite(L1_TTL_SECONDS, TimeUnit.SECONDS)
            .recordStats()
            .build();

    /** Caffeine 不允许 null 值，用包装类承载"空值哨兵" */
    private static final class LocalValue {
        private final Object value;

        private LocalValue(Object value) {
            this.value = value;
        }
    }

    public void set(String key, Object value, Long time, TimeUnit timeUnit) {
        try {
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),
                    withJitter(timeUnit.toSeconds(time)), TimeUnit.SECONDS);
        } catch (DataAccessException e) {
            REDIS_FAILURES.increment();
            log.warn("写入 Redis 缓存失败，保留 L1 数据，key={}", key, e);
        }
        localCache.put(key, new LocalValue(value));
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit timeUnit) {
        long logicalSeconds = timeUnit.toSeconds(time);
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(withJitter(logicalSeconds)));
        // 逻辑过期靠读取方触发重建；物理 TTL 必须远大于逻辑 TTL，只作兜底回收，
        // 否则"逻辑已过期但一直没人读"的 key 会永久残留，或提前物理消失导致穿透。
        long physicalSeconds = Math.max(logicalSeconds * 3, logicalSeconds + 3600);
        try {
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData),
                    withJitter(physicalSeconds), TimeUnit.SECONDS);
        } catch (DataAccessException e) {
            REDIS_FAILURES.increment();
            log.warn("写入 Redis 逻辑过期缓存失败，保留 L1 数据，key={}", key, e);
        }
        localCache.put(key, new LocalValue(value));
    }

    /**
     * 更新节点在数据库事务提交后调用：先删除共享 L2，再删除本机 L1，最后广播其它实例删除 L1。
     * Pub/Sub 只承担当次加速失效；即使通知丢失，L1 的短 TTL 仍会限制脏读窗口。
     */
    public void invalidate(String key) {
        invalidateSharedAndLocal(key);
    }

    /**
     * 写路径使用与缓存重建相同的锁。若重建先读到旧数据库值，更新提交后的删除会等待它写完再清理；
     * 若更新先完成删除，后续重建只能读到已经提交的新值。
     */
    public void invalidate(String key, String lockKey) {
        RLock lock = redissonClient.getLock(lockKey);
        lock.lock();
        try {
            invalidateSharedAndLocal(key);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void invalidateSharedAndLocal(String key) {
        stringRedisTemplate.delete(key);
        invalidateLocal(key);
        stringRedisTemplate.convertAndSend(RedisConstants.CACHE_INVALIDATE_CHANNEL, key);
    }

    /** 收到跨实例通知时只删除本地缓存，不能再次广播，否则会形成消息环路。 */
    public void invalidateLocal(String key) {
        if (StrUtil.isNotBlank(key)) {
            localCache.invalidate(key);
        }
    }

    /** L1 命中率等统计，供监控读取 */
    public CacheStats localCacheStats() {
        return localCache.stats();
    }

    public Map<String, Object> metricsSnapshot() {
        CacheStats l1 = localCache.stats();
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("l1EstimatedSize", localCache.estimatedSize());
        metrics.put("l1HitCount", l1.hitCount());
        metrics.put("l1MissCount", l1.missCount());
        metrics.put("l1HitRate", l1.hitRate());
        metrics.put("redisHitCount", REDIS_HITS.sum());
        metrics.put("redisMissCount", REDIS_MISSES.sum());
        metrics.put("redisFailureCount", REDIS_FAILURES.sum());
        metrics.put("nullHitCount", NULL_HITS.sum());
        metrics.put("staleReturnCount", STALE_RETURNS.sum());
        metrics.put("databaseLoadCount", DB_LOADS.sum());
        metrics.put("rebuildSucceeded", REBUILD_SUCCEEDED.sum());
        metrics.put("rebuildFailed", REBUILD_FAILED.sum());
        metrics.put("rebuildRejected", REBUILD_REJECTED.sum());
        metrics.put("rebuildQueueSize", CACHE_REBUILD_EXECUTOR.getQueue().size());
        metrics.put("dbDegradeAvailablePermits", DB_DEGRADE_GUARD.availablePermits());
        return metrics;
    }

    /** L1 查询：返回 null 表示未命中；命中空值哨兵时 value 为 null */
    private LocalValue localGet(String key) {
        return localCache.getIfPresent(key);
    }

    private <R> R unwrap(LocalValue cached, Class<R> type) {
        return cached.value == null ? null : type.cast(cached.value);
    }

    public <R, ID> R queryWithPassThrough(Long time, TimeUnit timeUnit, String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback) {
        String key = keyPrefix + id;
        // L1
        LocalValue cached = localGet(key);
        if (cached != null) {
            return unwrap(cached, type);
        }
        // L2
        String json;
        try {
            json = getRedisValue(key);
        } catch (DataAccessException e) {
            return loadFromDatabaseDegraded(key, id, dbFallback, e);
        }
        if (StrUtil.isNotBlank(json)) {
            R value = JSONUtil.toBean(json, type);
            localCache.put(key, new LocalValue(value));
            return value;
        }
        // 空串是空值哨兵（数据库确认不存在），与 key 不存在（null）区分
        if (json != null) {
            NULL_HITS.increment();
            localCache.put(key, new LocalValue(null));
            return null;
        }

        R r = loadFromDatabase(id, dbFallback);
        if (r == null) {
            setNullValue(key);
            return null;
        }
        this.set(key, r, time, timeUnit);
        return r;
    }

    public <R, ID> R queryWithLogicalExpire(Long time, TimeUnit timeUnit, String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback) {
        String key = keyPrefix + id;
        // L1：TTL 远短于逻辑过期时间，命中即可直接返回
        LocalValue cached = localGet(key);
        if (cached != null) {
            return unwrap(cached, type);
        }
        // L2
        String json;
        try {
            json = getRedisValue(key);
        } catch (DataAccessException e) {
            return loadFromDatabaseDegraded(key, id, dbFallback, e);
        }

        // 首次未命中：缓存里还没有这条数据
        if (StrUtil.isBlank(json)) {
            if (json != null) {
                // 空值哨兵：数据库确认不存在，直接返回
                NULL_HITS.increment();
                localCache.put(key, new LocalValue(null));
                return null;
            }
            // 首次查询同步回源，并把结果返回给调用方；
            // 不能像旧实现那样写完后仍返回 null，否则"未预热"会被上层误报成"不存在"。
            return loadColdCacheWithMutex(key, lockKeyFor(id), time, timeUnit, id, type, dbFallback);
        }

        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
            localCache.put(key, new LocalValue(r));
            return r;
        }

        // 逻辑已过期：拿锁的线程异步重建，其他线程先返回旧值（不阻塞、不穿透）
        STALE_RETURNS.increment();
        submitRebuild(key, lockKeyFor(id), time, timeUnit, id, type, dbFallback);
        return r;
    }

    /**
     * 逻辑过期 key 第一次访问时没有旧值可返回，因此同步构建；同一个 key 只允许一个请求回源数据库。
     * 等待者短暂轮询 Redis，重建完成后从 L2 读取并自然回填 L1。
     */
    private <R, ID> R loadColdCacheWithMutex(String key, String lockKey, Long time, TimeUnit timeUnit,
                                              ID id, Class<R> type, Function<ID, R> dbFallback) {
        try {
            for (int attempt = 0; attempt < MUTEX_MAX_RETRY; attempt++) {
                RLock lock = redissonClient.getLock(lockKey);
                if (lock.tryLock()) {
                    try {
                        String latestJson = stringRedisTemplate.opsForValue().get(key);
                        if (latestJson != null) {
                            return readLogicalValueAndFillLocal(key, latestJson, type);
                        }
                        return loadAndCacheWithLogicalExpire(key, time, timeUnit, id, type, dbFallback);
                    } finally {
                        if (lock.isHeldByCurrentThread()) {
                            lock.unlock();
                        }
                    }
                }
                sleepBeforeRetry();
                String latestJson = stringRedisTemplate.opsForValue().get(key);
                if (latestJson != null) {
                    return readLogicalValueAndFillLocal(key, latestJson, type);
                }
            }
        } catch (DataAccessException | RedisException e) {
            REDIS_FAILURES.increment();
            return loadFromDatabaseDegraded(key, id, dbFallback, e);
        }
        // 锁竞争超过约 250ms 后受限回源，避免等待者同时绕过缓存压垮数据库。
        log.warn("[缓存冷启动] 重试 {} 次仍未拿到锁，受限回源数据库，key={}", MUTEX_MAX_RETRY, key);
        return loadFromDatabaseGuarded(key, id, dbFallback,
                new IllegalStateException("缓存冷启动锁竞争超时"));
    }

    private <R> R readLogicalValueAndFillLocal(String key, String json, Class<R> type) {
        if (StrUtil.isBlank(json)) {
            localCache.put(key, new LocalValue(null));
            return null;
        }
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R value = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        // 冷启动等待期间若正好读到逻辑过期值，不放入 L1，避免把旧值再固定 30 秒。
        if (redisData.getExpireTime() != null && redisData.getExpireTime().isAfter(LocalDateTime.now())) {
            localCache.put(key, new LocalValue(value));
        }
        return value;
    }

    /** 逻辑过期：DB 不存在写空值哨兵，存在则写逻辑过期缓存并返回真实数据 */
    private <R, ID> R loadAndCacheWithLogicalExpire(String key, Long time, TimeUnit timeUnit,
                                                    ID id, Class<R> type, Function<ID, R> dbFallback) {
        R r = loadFromDatabase(id, dbFallback);
        if (r == null) {
            setNullValue(key);
            return null;
        }
        setWithLogicalExpire(key, r, time, timeUnit);
        return r;
    }

    private <R, ID> void submitRebuild(String key, String lockKey, Long time, TimeUnit timeUnit,
                                       ID id, Class<R> type, Function<ID, R> dbFallback) {
        if (!rebuildingKeys.add(key)) {
            return;
        }
        try {
            CACHE_REBUILD_EXECUTOR.execute(() -> {
                try {
                    rebuild(key, lockKey, time, timeUnit, id, type, dbFallback);
                } finally {
                    rebuildingKeys.remove(key);
                }
            });
        } catch (RejectedExecutionException e) {
            rebuildingKeys.remove(key);
            REBUILD_REJECTED.increment();
            log.error("[缓存重建] 线程池队列已满（上限 {}），丢弃本次重建，累计丢弃 {}",
                    REBUILD_QUEUE_CAPACITY, REBUILD_REJECTED.sum());
        }
    }

    private <R, ID> void rebuild(String key, String lockKey, Long time, TimeUnit timeUnit,
                                 ID id, Class<R> type, Function<ID, R> dbFallback) {
        RLock lock = redissonClient.getLock(lockKey);
        if (!lock.tryLock()) {
            return;
        }
        try {
            // Double Check：持锁后可能已被前一个持锁者重建完成，避免重复回源
            String json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(json)) {
                RedisData latest = JSONUtil.toBean(json, RedisData.class);
                if (latest.getExpireTime() != null && latest.getExpireTime().isAfter(LocalDateTime.now())) {
                    return;
                }
            } else if (json != null) {
                return;
            }
            loadAndCacheWithLogicalExpire(key, time, timeUnit, id, type, dbFallback);
            REBUILD_SUCCEEDED.increment();
        } catch (Exception e) {
            // 旧实现把异常重新抛进线程池，Future 被丢弃后完全无声；这里显式记录并计数
            REBUILD_FAILED.increment();
            log.error("[缓存重建] 失败，key={}，累计失败 {}", key, REBUILD_FAILED.sum(), e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private static final int MUTEX_MAX_RETRY = 5;
    private static final long MUTEX_RETRY_INTERVAL_MILLIS = 50L;

    public <R, ID> R queryWithMutex(Long time, TimeUnit timeUnit, String keyPrefix, ID id,
                                    Class<R> type, Function<ID, R> dbFallback) {
        try {
            return queryWithMutexInternal(time, timeUnit, keyPrefix, id, type, dbFallback);
        } catch (DataAccessException | RedisException e) {
            REDIS_FAILURES.increment();
            return loadFromDatabaseDegraded(keyPrefix + id, id, dbFallback, e);
        }
    }

    private <R, ID> R queryWithMutexInternal(Long time, TimeUnit timeUnit, String keyPrefix, ID id,
                                              Class<R> type, Function<ID, R> dbFallback) {
        String key = keyPrefix + id;
        // L1
        LocalValue cached = localGet(key);
        if (cached != null) {
            return unwrap(cached, type);
        }
        // L2
        String json;
        try {
            json = getRedisValue(key);
        } catch (DataAccessException e) {
            return loadFromDatabaseDegraded(key, id, dbFallback, e);
        }
        if (StrUtil.isNotBlank(json)) {
            R value = JSONUtil.toBean(json, type);
            localCache.put(key, new LocalValue(value));
            return value;
        }
        if (json != null) {
            NULL_HITS.increment();
            localCache.put(key, new LocalValue(null));
            return null;
        }

        String lockKey = lockKeyFor(id);
        // 旧实现用"未获锁就递归重试"，每层 finally 都会 unlock 一次，
        // 会删掉真正持锁者的锁；这里改成有上限的循环，且只在持锁的帧里释放自己的 token。
        for (int attempt = 0; attempt < MUTEX_MAX_RETRY; attempt++) {
            RLock lock = redissonClient.getLock(lockKey);
            if (!lock.tryLock()) {
                sleepBeforeRetry();
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                json = stringRedisTemplate.opsForValue().get(key);
                if (StrUtil.isNotBlank(json)) {
                    R value = JSONUtil.toBean(json, type);
                    localCache.put(key, new LocalValue(value));
                    return value;
                }
                if (json != null) {
                    localCache.put(key, new LocalValue(null));
                    return null;
                }
                continue;
            }
            try {
                // Double Check：持锁后可能已被上一个持锁者写好
                json = stringRedisTemplate.opsForValue().get(key);
                if (StrUtil.isNotBlank(json)) {
                    R value = JSONUtil.toBean(json, type);
                    localCache.put(key, new LocalValue(value));
                    return value;
                }
                if (json != null) {
                    localCache.put(key, new LocalValue(null));
                    return null;
                }
                R r = loadFromDatabase(id, dbFallback);
                if (r == null) {
                    setNullValue(key);
                    return null;
                }
                set(key, r, time, timeUnit);
                return r;
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }
        // 重试耗尽：受限回源数据库，避免锁竞争把正常数据判成“不存在”，也避免数据库被并发打满。
        log.warn("[缓存互斥] 重试 {} 次仍未拿到锁，受限回源数据库，key={}", MUTEX_MAX_RETRY, key);
        return loadFromDatabaseGuarded(key, id, dbFallback,
                new IllegalStateException("缓存互斥锁竞争超时"));
    }

    private String lockKeyFor(Object id) {
        return RedisConstants.LOCK_SHOP_KEY + id;
    }

    private void sleepBeforeRetry() {
        try {
            Thread.sleep(MUTEX_RETRY_INTERVAL_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 空值哨兵：空串与"key 不存在"可区分，短 TTL 防止空值长期占用；同步写入 L1 */
    private void setNullValue(String key) {
        long ttlSeconds = withJitter(TimeUnit.MINUTES.toSeconds(RedisConstants.CACHE_NULL_TTL));
        try {
            stringRedisTemplate.opsForValue().set(key, "", ttlSeconds, TimeUnit.SECONDS);
        } catch (DataAccessException e) {
            REDIS_FAILURES.increment();
            log.warn("写入 Redis 空值缓存失败，保留 L1 空值，key={}", key, e);
        }
        localCache.put(key, new LocalValue(null));
    }

    private String getRedisValue(String key) {
        try {
            String value = stringRedisTemplate.opsForValue().get(key);
            if (value == null) {
                REDIS_MISSES.increment();
            } else {
                REDIS_HITS.increment();
            }
            return value;
        } catch (DataAccessException e) {
            REDIS_FAILURES.increment();
            throw e;
        }
    }

    private <R, ID> R loadFromDatabase(ID id, Function<ID, R> dbFallback) {
        DB_LOADS.increment();
        return dbFallback.apply(id);
    }

    /** Redis 故障时只允许固定数量请求同时绕过缓存，防止瞬时流量直接压垮 MySQL。 */
    private <R, ID> R loadFromDatabaseDegraded(String key, ID id, Function<ID, R> dbFallback,
                                                RuntimeException redisFailure) {
        return loadFromDatabaseGuarded(key, id, dbFallback, redisFailure);
    }

    private <R, ID> R loadFromDatabaseGuarded(String key, ID id, Function<ID, R> dbFallback,
                                               RuntimeException fallbackCause) {
        if (!DB_DEGRADE_GUARD.tryAcquire()) {
            throw new IllegalStateException("缓存回源数据库的并发已满", fallbackCause);
        }
        try {
            log.warn("缓存降级，受限回源数据库，key={}", key);
            R value = loadFromDatabase(id, dbFallback);
            localCache.put(key, new LocalValue(value));
            return value;
        } finally {
            DB_DEGRADE_GUARD.release();
        }
    }

    /**
     * TTL 随机化：避免同一批 key 同时失效造成缓存雪崩。
     * 抖动 ±10%，秒级以下不抖。
     */
    private long withJitter(long seconds) {
        if (seconds <= 1) {
            return seconds;
        }
        long bound = Math.max(1, seconds / 10);
        return seconds + ThreadLocalRandom.current().nextLong(-bound, bound + 1);
    }

    /** 命名 daemon 线程工厂，便于 jstack 定位缓存重建线程 */
    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger sequence = new AtomicInteger(1);

        private NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, prefix + sequence.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }
}
