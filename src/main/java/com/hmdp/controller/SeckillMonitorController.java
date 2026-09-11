package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.monitor.SeckillStatsService;
import com.hmdp.monitor.SeckillTimelineService;
import com.hmdp.service.impl.VoucherOrderServiceImpl_kafka;
import com.hmdp.utils.SeckillMessageReplayService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.Map;

/**
 * 秒杀链路观测与人工处置端点。
 * 访问需要同时满足：已登录（LoginInterceptor）+ 携带管理员令牌请求头
 * {@code X-Monitor-Token}（MonitorAdminInterceptor，令牌取自配置 seckill.monitor.admin-token）。
 * 未配置令牌时整组端点 403，避免批量重放这类处置能力暴露给普通用户。
 */
@RestController
@RequestMapping("/monitor")
public class SeckillMonitorController {

    @Resource
    private SeckillStatsService seckillStatsService;

    @Resource
    private SeckillTimelineService seckillTimelineService;

    @Resource
    private SeckillMessageReplayService seckillMessageReplayService;

    @Resource(name = "voucherOrderServiceImpl_kafka")
    private VoucherOrderServiceImpl_kafka voucherOrderService;

    @GetMapping("/seckill")
    public Result seckillStats() {
        return Result.ok(seckillStatsService.snapshot());
    }

    /** 面向管理后台的链路视图：中文阶段漏斗 + 近期每一笔订单所处阶段 */
    @GetMapping("/seckill/pipeline")
    public Result seckillPipeline() {
        return Result.ok(seckillStatsService.pipeline());
    }

    /** 按订单号还原整条链路时间线（Redis 预扣→outbox→Kafka→消费落库→重试/兜底/对账） */
    @GetMapping("/seckill/timeline/{orderId}")
    public Result seckillTimeline(@PathVariable("orderId") Long orderId) {
        Map<String, Object> timeline = seckillTimelineService.timeline(orderId);
        return timeline == null ? Result.fail("未找到该订单的秒杀消息记录") : Result.ok(timeline);
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

    /** 工作人员按完整核销码查询预约，接口受登录与管理员令牌双重保护。 */
    @GetMapping("/workshop-orders/{verificationCode}")
    public Result queryWorkshopOrder(@PathVariable("verificationCode") String verificationCode) {
        return voucherOrderService.queryWorkshopOrderForRedeem(verificationCode);
    }

    /** 工作人员确认到店后核销，仅允许预约状态从 2 原子更新为 3。 */
    @PostMapping("/workshop-orders/{verificationCode}/redeem")
    public Result redeemWorkshopOrder(@PathVariable("verificationCode") String verificationCode) {
        return voucherOrderService.redeemWorkshopOrder(verificationCode);
    }
}
