package com.hmdp.exception;

/**
 * 触发限流。由 RateLimitExceptionAdvice 统一转成 HTTP 429 + Result.fail。
 */
public class RateLimitException extends RuntimeException {

    public RateLimitException(String message) {
        super(message);
    }
}
