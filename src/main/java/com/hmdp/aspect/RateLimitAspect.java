package com.hmdp.aspect;

import com.hmdp.annotation.RateLimit;
import com.hmdp.exception.RateLimitException;
import com.hmdp.utils.TokenBucketRateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.lang.reflect.Method;

/**
 * 把 {@link RateLimit} 注解翻译成一次令牌桶尝试。
 * 维度值用 SpEL 从方法参数里取——因为像发送验证码这种接口是免登录的，
 * ThreadLocal 里没有用户，只能按入参（手机号）限流。
 */
@Aspect
@Component
@Slf4j
public class RateLimitAspect {

    private static final String KEY_PREFIX = "rate:limit:";

    private final ExpressionParser parser = new SpelExpressionParser();
    private final DefaultParameterNameDiscoverer nameDiscoverer = new DefaultParameterNameDiscoverer();

    @Resource
    private TokenBucketRateLimiter rateLimiter;

    @Around("@annotation(rateLimit)")
    public Object around(ProceedingJoinPoint pjp, RateLimit rateLimit) throws Throwable {
        String key = KEY_PREFIX + rateLimit.keyPrefix() + ":" + resolveIdentity(pjp, rateLimit);
        if (!rateLimiter.tryAcquire(key, rateLimit.capacity(), rateLimit.rate())) {
            log.warn("触发限流：key={}, capacity={}, rate={}/s",
                    key, rateLimit.capacity(), rateLimit.rate());
            throw new RateLimitException(rateLimit.message());
        }
        return pjp.proceed();
    }

    /** 解析 SpEL 维度值；表达式为空表示全局共用一个桶 */
    private String resolveIdentity(ProceedingJoinPoint pjp, RateLimit rateLimit) {
        if (rateLimit.key().isEmpty()) {
            return "all";
        }
        MethodSignature signature = (MethodSignature) pjp.getSignature();
        // 走接口方法时参数名可能拿不到，取实际实现类的方法
        Method method = AopUtils.getMostSpecificMethod(
                signature.getMethod(), pjp.getTarget().getClass());
        String[] names = nameDiscoverer.getParameterNames(method);
        Object[] args = pjp.getArgs();

        StandardEvaluationContext context = new StandardEvaluationContext();
        if (names != null) {
            for (int i = 0; i < names.length && i < args.length; i++) {
                context.setVariable(names[i], args[i]);
            }
        }
        // 字节码里没有参数名时（未用 -parameters 编译）用位置兜底：#p0 / #a0
        for (int i = 0; i < args.length; i++) {
            context.setVariable("p" + i, args[i]);
            context.setVariable("a" + i, args[i]);
        }

        Expression expression = parser.parseExpression(rateLimit.key());
        Object value = expression.getValue(context);
        return value == null ? "unknown" : value.toString();
    }
}
