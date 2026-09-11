package com.hmdp.service;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.monitor.SeckillEventLogger;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.StreamUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collections;

@Service
@Slf4j
public class VoucherOrderLifecycleService {

    public static final String CLOSE_REASON_USER_CANCEL = "USER_CANCEL";
    public static final String CLOSE_REASON_PAYMENT_TIMEOUT = "PAYMENT_TIMEOUT";
    private static final long CLOSE_RETRY_BASE_SECONDS = 5L;
    private static final long CLOSE_RETRY_MAX_SECONDS = 3600L;

    private static final DefaultRedisScript<Long> CLOSE_SCRIPT;

    static {
        CLOSE_SCRIPT = new DefaultRedisScript<>();
        CLOSE_SCRIPT.setScriptText(readScriptText("close-voucher-order.lua"));
        CLOSE_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    @Resource
    private SeckillVoucherMapper seckillVoucherMapper;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SeckillEventLogger seckillEventLogger;

    @Value("${workshop.order.payment-timeout-seconds:900}")
    private long paymentTimeoutSeconds;

    @Transactional(rollbackFor = Exception.class)
    public Result payMyOrder(Long orderId, Integer payType) {
        Long userId = UserHolder.getUser().getId();
        VoucherOrder order = voucherOrderMapper.selectById(orderId);
        if (order == null || !userId.equals(order.getUserId())) {
            return Result.fail("预约订单不存在");
        }
        if (Integer.valueOf(VoucherOrder.STATUS_PENDING_REDEMPTION).equals(order.getStatus())
                || Integer.valueOf(VoucherOrder.STATUS_REDEEMED).equals(order.getStatus())) {
            return Result.ok(order);
        }
        if (!Integer.valueOf(VoucherOrder.STATUS_PENDING_PAYMENT).equals(order.getStatus())) {
            return Result.fail(statusMessage(order.getStatus()));
        }
        if (payType == null || payType < 1 || payType > 3) {
            return Result.fail("支付方式不正确");
        }

        int updated = voucherOrderMapper.update(null, new UpdateWrapper<VoucherOrder>()
                .eq("id", orderId)
                .eq("user_id", userId)
                .eq("status", VoucherOrder.STATUS_PENDING_PAYMENT)
                // 使用数据库时钟并把截止条件放进同一条 CAS，避免多实例时钟偏差和校验/更新竞态。
                .apply("create_time > DATE_SUB(NOW(), INTERVAL {0} SECOND)", paymentTimeoutSeconds)
                .set("status", VoucherOrder.STATUS_PENDING_REDEMPTION)
                .set("pay_type", payType)
                .setSql("pay_time = NOW(), update_time = NOW()"));
        if (updated == 0) {
            // 可能是重复点击，也可能是支付与自动关单并发。重新读取数据库真值：
            // 已支付视为幂等成功；关单获胜则明确告知用户本次支付没有生效。
            VoucherOrder latest = voucherOrderMapper.selectById(orderId);
            if (latest != null && (Integer.valueOf(VoucherOrder.STATUS_PENDING_REDEMPTION).equals(latest.getStatus())
                    || Integer.valueOf(VoucherOrder.STATUS_REDEEMED).equals(latest.getStatus()))) {
                return Result.ok(latest);
            }
            if (latest != null && (Integer.valueOf(VoucherOrder.STATUS_CLOSING).equals(latest.getStatus())
                    || Integer.valueOf(VoucherOrder.STATUS_CANCELLED).equals(latest.getStatus()))) {
                return Result.fail("订单已由关单流程锁定，本次支付未生效");
            }
            if (latest != null && Integer.valueOf(VoucherOrder.STATUS_PENDING_PAYMENT).equals(latest.getStatus())) {
                return Result.fail("支付时限已过，订单正在关闭");
            }
            return Result.fail("支付结果未确认，请刷新订单状态");
        }
        return Result.ok(voucherOrderMapper.selectById(orderId));
    }

    @Transactional(rollbackFor = Exception.class)
    public Result cancelMyOrder(Long orderId) {
        Long userId = UserHolder.getUser().getId();
        VoucherOrder order = voucherOrderMapper.selectById(orderId);
        if (order == null || !userId.equals(order.getUserId())) {
            return Result.fail("预约订单不存在");
        }
        if (Integer.valueOf(VoucherOrder.STATUS_CANCELLED).equals(order.getStatus())
                || Integer.valueOf(VoucherOrder.STATUS_CLOSING).equals(order.getStatus())) {
            return Result.ok();
        }
        if (!Integer.valueOf(VoucherOrder.STATUS_PENDING_PAYMENT).equals(order.getStatus())) {
            return Result.fail("只有待支付预约可以取消");
        }
        return markClosing(order, CLOSE_REASON_USER_CANCEL, false)
                ? Result.ok("取消请求已受理")
                : Result.fail("订单状态已变化，请刷新后重试");
    }

    @Transactional(rollbackFor = Exception.class)
    public boolean closeExpiredOrder(Long orderId) {
        VoucherOrder order = voucherOrderMapper.selectById(orderId);
        if (order == null || !Integer.valueOf(VoucherOrder.STATUS_PENDING_PAYMENT).equals(order.getStatus())) {
            return false;
        }
        return markClosing(order, CLOSE_REASON_PAYMENT_TIMEOUT, true);
    }

    public void compensateClosingOrder(Long orderId) {
        VoucherOrder order = voucherOrderMapper.selectById(orderId);
        if (order == null || !Integer.valueOf(VoucherOrder.STATUS_CLOSING).equals(order.getStatus())) {
            return;
        }
        try {
            Long result = stringRedisTemplate.execute(
                    CLOSE_SCRIPT,
                    Collections.emptyList(),
                    String.valueOf(order.getVoucherId()),
                    String.valueOf(order.getUserId()),
                    String.valueOf(order.getId()));
            if (result == null) {
                throw new IllegalStateException("Redis 关单补偿返回 null");
            }
            int completed = voucherOrderMapper.update(null, new UpdateWrapper<VoucherOrder>()
                    .eq("id", orderId)
                    .eq("status", VoucherOrder.STATUS_CLOSING)
                    .set("status", VoucherOrder.STATUS_CANCELLED)
                    .set("close_next_retry_time", null)
                    .set("close_last_error", null)
                    .setSql("update_time = NOW()"));
            log.info("预约订单关单补偿完成，orderId={}, voucherId={}, redisFirstRun={}, statusChanged={}",
                    orderId, order.getVoucherId(), result == 1L, completed == 1);
            if (completed == 1) {
                seckillEventLogger.info(orderId, order.getUserId(), order.getVoucherId(),
                        SeckillEventLogger.STAGE_ORDER_CLOSED,
                        "订单已关闭，MySQL 与 Redis 名额各释放 1");
            }
        } catch (RuntimeException e) {
            seckillEventLogger.warn(orderId, order.getUserId(), order.getVoucherId(),
                    SeckillEventLogger.STAGE_ORDER_CLOSE_FAILED,
                    "释放名额失败，保留 CLOSING 退避重试：" + e.getMessage());
            recordCompensationFailure(order, e);
        }
    }

    public LocalDateTime paymentDeadline(VoucherOrder order) {
        return order.getCreateTime() == null
                ? LocalDateTime.now()
                : order.getCreateTime().plusSeconds(paymentTimeoutSeconds);
    }

    private boolean markClosing(VoucherOrder order, String reason, boolean requireExpired) {
        UpdateWrapper<VoucherOrder> wrapper = new UpdateWrapper<VoucherOrder>()
                .eq("id", order.getId())
                .eq("status", VoucherOrder.STATUS_PENDING_PAYMENT)
                .set("status", VoucherOrder.STATUS_CLOSING)
                .set("close_reason", reason)
                .set("close_retry", 0)
                .setSql("close_next_retry_time = NOW(), close_last_error = NULL, update_time = NOW()");
        if (requireExpired) {
            wrapper.apply("create_time <= DATE_SUB(NOW(), INTERVAL {0} SECOND)", paymentTimeoutSeconds);
        }
        int updated = voucherOrderMapper.update(null, wrapper);
        if (updated == 0) {
            return false;
        }

        int stockRestored = seckillVoucherMapper.update(null, new UpdateWrapper<SeckillVoucher>()
                .eq("voucher_id", order.getVoucherId())
                .setSql("stock = stock + 1"));
        if (stockRestored == 0) {
            throw new IllegalStateException("关单恢复 MySQL 库存失败，voucherId=" + order.getVoucherId());
        }

        Runnable compensation = () -> {
            // 事件必须在事务提交后写（与 VoucherOrderConsumer 一致），否则回滚会留下假的关单事件
            seckillEventLogger.info(order.getId(), order.getUserId(), order.getVoucherId(),
                    SeckillEventLogger.STAGE_ORDER_CLOSE_REQUESTED,
                    CLOSE_REASON_USER_CANCEL.equals(reason)
                            ? "用户取消预约，开始释放名额"
                            : "支付超时，开始释放名额");
            compensateClosingOrder(order.getId());
        };
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
                @Override
                public void afterCommit() {
                    compensation.run();
                }
            });
        } else {
            compensation.run();
        }
        return true;
    }

    private void recordCompensationFailure(VoucherOrder order, RuntimeException failure) {
        int retry = order.getCloseRetry() == null ? 0 : order.getCloseRetry();
        long delay = Math.min(CLOSE_RETRY_MAX_SECONDS,
                CLOSE_RETRY_BASE_SECONDS * (1L << Math.min(retry, 9)));
        String error = failure.getMessage();
        if (error != null && error.length() > 500) {
            error = error.substring(0, 500);
        }
        try {
            voucherOrderMapper.update(null, new UpdateWrapper<VoucherOrder>()
                    .eq("id", order.getId())
                    .eq("status", VoucherOrder.STATUS_CLOSING)
                    .set("close_last_error", error)
                    .setSql("close_retry = close_retry + 1, close_next_retry_time = "
                            + "DATE_ADD(NOW(), INTERVAL " + delay + " SECOND), update_time = NOW()"));
        } catch (RuntimeException recordFailure) {
            failure.addSuppressed(recordFailure);
        }
        log.error("预约订单 Redis 关单补偿失败，保留状态 7 退避重试，orderId={}, retry={}, delay={}s",
                order.getId(), retry + 1, delay, failure);
    }

    private String statusMessage(Integer status) {
        if (Integer.valueOf(VoucherOrder.STATUS_CLOSING).equals(status)) {
            return "订单正在关闭";
        }
        if (Integer.valueOf(VoucherOrder.STATUS_CANCELLED).equals(status)) {
            return "订单已取消";
        }
        return "当前订单状态不能支付";
    }

    private static String readScriptText(String classpathLocation) {
        try (InputStream in = new ClassPathResource(classpathLocation).getInputStream()) {
            return StreamUtils.copyToString(in, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("加载 Lua 脚本失败: " + classpathLocation, e);
        }
    }
}
