package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.entity.CacheInvalidationEvent;
import com.hmdp.mapper.CacheInvalidationEventMapper;
import com.hmdp.utils.CacheClient;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.Map;

/** 二级缓存运行指标；由 /monitor/** 管理员拦截器保护。 */
@RestController
@RequestMapping("/monitor/cache")
public class CacheMonitorController {

    @Resource
    private CacheClient cacheClient;

    @Resource
    private CacheInvalidationEventMapper eventMapper;

    @GetMapping
    public Result snapshot() {
        Map<String, Object> metrics = cacheClient.metricsSnapshot();
        metrics.put("invalidationPendingCount", eventMapper.selectCount(
                new QueryWrapper<CacheInvalidationEvent>()
                        .eq("status", CacheInvalidationEvent.STATUS_PENDING)));
        return Result.ok(metrics);
    }
}
