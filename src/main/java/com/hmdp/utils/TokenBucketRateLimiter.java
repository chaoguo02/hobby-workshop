package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * 基于 Redis + Lua 的集中式令牌桶（多实例共享同一个桶）。
 * 桶的状态放在 Redis 而非应用内存，所以集群下限流阈值是全局的；
 * 用 Guava RateLimiter / Sentinel 单机模式的话，每台机器各一个桶，实际阈值会放大 N 倍。
 */
@Component
@Slf4j
public class TokenBucketRateLimiter {

    private static final DefaultRedisScript<List> TOKEN_BUCKET_SCRIPT;

    static {
        TOKEN_BUCKET_SCRIPT = new DefaultRedisScript<>();
        // 与 seckill.lua 同理：脚本内容只读一次并常驻内存。
        // setLocation(ClassPathResource) 会让每次执行都重新读 fat jar 内的资源（要过 JarFile 全局锁），
        // 在高频限流场景下同样会把请求串行化。
        TOKEN_BUCKET_SCRIPT.setScriptText(readScriptText("token-bucket.lua"));
        TOKEN_BUCKET_SCRIPT.setResultType(List.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * @param key           桶标识，建议 rate:limit:{接口}:{维度值}
     * @param capacity      桶容量，决定允许的瞬时突发次数
     * @param ratePerSecond 每秒补充的令牌数，决定长期平均速率
     * @return true 放行；false 拒绝
     */
    public boolean tryAcquire(String key, int capacity, double ratePerSecond) {
        List<?> result = stringRedisTemplate.execute(
                TOKEN_BUCKET_SCRIPT,
                Collections.singletonList(key),
                String.valueOf(capacity),
                String.valueOf(ratePerSecond),
                String.valueOf(System.currentTimeMillis()),
                "1");
        if (result == null || result.isEmpty()) {
            // 脚本未执行（连接异常等）。限流保护的通常是有成本或有副作用的接口，故 fail closed。
            log.error("令牌桶脚本返回 null，按拒绝处理，key={}", key);
            return false;
        }
        return ((Number) result.get(0)).longValue() == 1L;
    }

    private static String readScriptText(String classpathLocation) {
        try (InputStream in = new ClassPathResource(classpathLocation).getInputStream()) {
            return StreamUtils.copyToString(in, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("加载 Lua 脚本失败: " + classpathLocation, e);
        }
    }
}
