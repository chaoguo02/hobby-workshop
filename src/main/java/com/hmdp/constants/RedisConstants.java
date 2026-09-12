package com.hmdp.constants;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 5L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    public static final Long CACHE_SHOPTYPE_TTL = 30L;
    public static final String CACHE_SHOPTYPE_KEY = "cache:shoptype:";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;
    public static final String CACHE_INVALIDATE_CHANNEL = "cache:invalidate";

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    /** 一人一单集合，Lua 里跟随库存 key 的 TTL 过期 */
    public static final String SECKILL_ORDER_KEY = "seckill:order:";
    /** 已准入 orderId→userId，恢复任务据此补齐 outbox */
    public static final String SECKILL_ADMITTED_KEY = "seckill:admitted:";
    /** 已完成关单补偿的 orderId 集合，用于 Redis 库存回补幂等 */
    public static final String SECKILL_CLOSED_KEY = "seckill:closed:";
    public static final String USER_SIGN_KEY = "sign:";

}
