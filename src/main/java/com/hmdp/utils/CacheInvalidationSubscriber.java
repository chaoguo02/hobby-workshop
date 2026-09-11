package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/** 接收其它应用实例发布的缓存失效消息，只清理当前 JVM 的 Caffeine。 */
@Component
@Slf4j
public class CacheInvalidationSubscriber implements MessageListener {

    private static final StringRedisSerializer STRING_SERIALIZER = new StringRedisSerializer();

    @Resource
    private CacheClient cacheClient;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String key = STRING_SERIALIZER.deserialize(message.getBody());
        if (key == null || key.isEmpty()) {
            log.warn("收到空的缓存失效消息，忽略");
            return;
        }
        cacheClient.invalidateLocal(key);
        log.debug("已根据广播清理本机 L1 缓存，key={}", key);
    }
}
