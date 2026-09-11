package com.hmdp.config;

import com.hmdp.constants.RedisConstants;
import com.hmdp.utils.CacheInvalidationSubscriber;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/** Redis Pub/Sub 只用于通知各实例清理本地 Caffeine，Redis 数据本身仍由更新节点删除。 */
@Configuration
public class CacheInvalidationConfig {

    @Bean
    public RedisMessageListenerContainer cacheInvalidationListenerContainer(
            RedisConnectionFactory connectionFactory,
            CacheInvalidationSubscriber subscriber) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(subscriber, new ChannelTopic(RedisConstants.CACHE_INVALIDATE_CHANNEL));
        return container;
    }
}
