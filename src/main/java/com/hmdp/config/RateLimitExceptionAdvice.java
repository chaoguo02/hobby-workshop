package com.hmdp.config;

import com.hmdp.dto.Result;
import com.hmdp.exception.RateLimitException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 限流异常的专用处理。比 WebExceptionAdvice 里的 RuntimeException 更具体，会被优先匹配，
 * 因此限流不会退化成「服务器异常」。
 */
@Slf4j
@RestControllerAdvice
public class RateLimitExceptionAdvice {

    @ExceptionHandler(RateLimitException.class)
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    public Result handleRateLimit(RateLimitException e) {
        log.warn("请求被限流：{}", e.getMessage());
        return Result.fail(e.getMessage());
    }
}
