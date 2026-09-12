package com.hmdp.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 令牌桶限流。写在 Controller 方法上，由 {@code RateLimitAspect} 拦截。
 *
 * <p>例子：验证码接口按手机号限流「平均每 60 秒一次」：
 * <pre>
 * &#64;RateLimit(keyPrefix = "sms:send", key = "#phone", capacity = 1, rate = 1.0 / 60)
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RateLimit {

    /** 桶前缀，用来区分不同接口，如 "sms:send" */
    String keyPrefix();

    /**
     * SpEL 表达式，从方法参数里取限流维度，如 "#phone"、"#userId"、"#dto.id"。
     * 留空表示所有调用共用一个桶（全局限流）。
     */
    String key() default "";

    /** 桶容量：允许的瞬时突发次数。填 1 表示完全不允许突发 */
    int capacity() default 5;

    /** 每秒补充的令牌数：决定长期平均速率。1.0 / 60 表示平均每 60 秒一次 */
    double rate() default 1;

    /** 被限流时返回给用户的提示 */
    String message() default "操作太频繁，请稍后再试";
}
