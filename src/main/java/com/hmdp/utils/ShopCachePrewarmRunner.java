package com.hmdp.utils;

import com.hmdp.constants.RedisConstants;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** 根据配置预热明确的热点店铺；默认关闭，不做全表扫描。 */
@Component
@Slf4j
public class ShopCachePrewarmRunner implements ApplicationRunner {

    @Value("${cache.prewarm.enabled:false}")
    private boolean enabled;

    @Value("${cache.prewarm.shop-ids:}")
    private String configuredShopIds;

    @Resource
    private ShopMapper shopMapper;

    @Resource
    private CacheClient cacheClient;

    @Override
    public void run(ApplicationArguments args) {
        if (!enabled || !StringUtils.hasText(configuredShopIds)) {
            return;
        }
        List<Long> ids = parseIds(configuredShopIds);
        if (ids.isEmpty()) {
            return;
        }
        List<Shop> shops = shopMapper.selectBatchIds(ids);
        for (Shop shop : shops) {
            cacheClient.setWithLogicalExpire(
                    RedisConstants.CACHE_SHOP_KEY + shop.getId(),
                    shop,
                    RedisConstants.CACHE_SHOP_TTL,
                    TimeUnit.MINUTES);
        }
        log.info("热点店铺缓存预热完成，配置 {} 条，成功 {} 条", ids.size(), shops.size());
    }

    private List<Long> parseIds(String value) {
        List<Long> ids = new ArrayList<>();
        for (String item : value.split(",")) {
            String trimmed = item.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                ids.add(Long.valueOf(trimmed));
            } catch (NumberFormatException e) {
                log.warn("忽略非法的热点店铺 ID：{}", trimmed);
            }
        }
        return ids;
    }
}
