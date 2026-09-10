package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.monitor.SeckillStatsService;
import com.hmdp.utils.SeckillMessageReplayService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * 秒杀链路观测与人工处置端点（需登录，未加入拦截器白名单）。
 */
@RestController
@RequestMapping("/monitor")
public class SeckillMonitorController {

    @Resource
    private SeckillStatsService seckillStatsService;

    @Resource
    private SeckillMessageReplayService seckillMessageReplayService;

    @GetMapping("/seckill")
    public Result seckillStats() {
        return Result.ok(seckillStatsService.snapshot());
    }

    /** 把一条 FAILED 消息重置回 READY，交给 relay 重新投递；返回是否命中 */
    @PostMapping("/seckill/replay/{orderId}")
    public Result replay(@PathVariable("orderId") Long orderId) {
        return Result.ok(seckillMessageReplayService.replay(orderId));
    }

    /** 批量重放 FAILED（最老优先，上限 500），返回实际重置条数 */
    @PostMapping("/seckill/replay-failed")
    public Result replayFailed(@RequestParam(value = "limit", defaultValue = "100") int limit) {
        return Result.ok(seckillMessageReplayService.replayAllFailed(limit));
    }
}
