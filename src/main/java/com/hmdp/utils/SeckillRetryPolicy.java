package com.hmdp.utils;

import com.hmdp.entity.SeckillMessage;

/**
 * 秒杀消息重投的退避策略。集中在这里，供 relay（投递失败退避）与
 * ReconcileTask（卡死消息重置退避）共用，避免两处各写一份阈值导致行为漂移。
 *
 * 分两段：
 * - 常规段（retry &lt; ALERT_THRESHOLD）：指数退避，封顶 {@link #MAX_FAST_BACKOFF_SECONDS}；
 * - 告警段（retry &gt;= ALERT_THRESHOLD）：更长的指数退避，封顶 {@link #MAX_SLOW_BACKOFF_SECONDS}。
 * 告警段没有「放弃」终点——FAILED 只是进入慢速通道并告警，后台仍会持续自动重试，
 * 因此「每笔已准入的资格最终都会自动落库」不依赖人工介入。
 */
public final class SeckillRetryPolicy {

    /** 超过该重投次数即告警并进入慢速通道 */
    public static final int ALERT_THRESHOLD = 3;
    /** 常规段退避上限 */
    public static final long MAX_FAST_BACKOFF_SECONDS = 300L;
    /** 告警段退避上限（1 小时） */
    public static final long MAX_SLOW_BACKOFF_SECONDS = 3600L;
    /** 告警段退避起步值（5 分钟） */
    private static final long SLOW_BACKOFF_BASE_SECONDS = 300L;

    private SeckillRetryPolicy() {
    }

    /** 未达告警阈值的常规退避：1、2、4、… 秒，封顶 300s */
    public static long fastBackoffSeconds(int retry) {
        return Math.min(MAX_FAST_BACKOFF_SECONDS, 1L << Math.min(retry, 8));
    }

    /** 达到告警阈值后的慢速退避：5、10、20、40 分钟，封顶 1h */
    public static long slowBackoffSeconds(int retry) {
        long steps = Math.min(Math.max(retry - ALERT_THRESHOLD, 0), 6);
        return Math.min(MAX_SLOW_BACKOFF_SECONDS, SLOW_BACKOFF_BASE_SECONDS << steps);
    }

    /** 按当前 retry 次数选择退避时长 */
    public static long backoffSeconds(int retry) {
        return retry < ALERT_THRESHOLD ? fastBackoffSeconds(retry) : slowBackoffSeconds(retry);
    }

    /** retry 达到阈值后应处于的状态：慢速通道用 FAILED，否则用 READY */
    public static int statusForRetry(int retry) {
        return retry < ALERT_THRESHOLD
                ? SeckillMessage.STATUS_READY
                : SeckillMessage.STATUS_FAILED;
    }
}
