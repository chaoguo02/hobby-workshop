package com.hmdp.service;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.monitor.SeckillEventLogger;
import com.hmdp.utils.VoucherOrderCloseTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VoucherOrderCloseStabilityTest {

    @Mock
    private VoucherOrderMapper voucherOrderMapper;
    @Mock
    private SeckillVoucherMapper seckillVoucherMapper;
    @Mock
    private StringRedisTemplate redisTemplate;
    @Mock
    private SeckillEventLogger seckillEventLogger;

    private VoucherOrderLifecycleService lifecycleService;

    @BeforeEach
    void setUp() {
        lifecycleService = new VoucherOrderLifecycleService();
        ReflectionTestUtils.setField(lifecycleService, "voucherOrderMapper", voucherOrderMapper);
        ReflectionTestUtils.setField(lifecycleService, "seckillVoucherMapper", seckillVoucherMapper);
        ReflectionTestUtils.setField(lifecycleService, "stringRedisTemplate", redisTemplate);
        ReflectionTestUtils.setField(lifecycleService, "seckillEventLogger", seckillEventLogger);
        ReflectionTestUtils.setField(lifecycleService, "paymentTimeoutSeconds", 900L);
    }

    @SuppressWarnings("unchecked")
    @Test
    void expiredOrderShouldRestoreMysqlAndRedisOnlyOnceThroughClosingState() {
        VoucherOrder order = order(1L);
        VoucherOrder closingOrder = order(1L).setStatus(VoucherOrder.STATUS_CLOSING);
        when(voucherOrderMapper.selectById(1L)).thenReturn(order, closingOrder);
        when(voucherOrderMapper.update(isNull(), any(Wrapper.class))).thenReturn(1, 1);
        when(seckillVoucherMapper.update(isNull(), any(Wrapper.class))).thenReturn(1);
        when(redisTemplate.execute(any(), anyList(), eq("100"), eq("11"), eq("1"))).thenReturn(1L);

        assertTrue(lifecycleService.closeExpiredOrder(1L));

        verify(seckillVoucherMapper).update(isNull(), any(Wrapper.class));
        verify(redisTemplate).execute(any(), anyList(), eq("100"), eq("11"), eq("1"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void oneBrokenOrderMustNotStopTheRemainingBatch() {
        VoucherOrder first = order(1L);
        VoucherOrder second = order(2L);
        when(voucherOrderMapper.selectList(any(Wrapper.class)))
                .thenReturn(Arrays.asList(first, second), Collections.emptyList());

        VoucherOrderLifecycleService mockedLifecycle = org.mockito.Mockito.mock(VoucherOrderLifecycleService.class);
        doThrow(new IllegalStateException("single order failed"))
                .when(mockedLifecycle).closeExpiredOrder(1L);

        VoucherOrderCloseTask task = new VoucherOrderCloseTask();
        ReflectionTestUtils.setField(task, "voucherOrderMapper", voucherOrderMapper);
        ReflectionTestUtils.setField(task, "lifecycleService", mockedLifecycle);
        ReflectionTestUtils.setField(task, "paymentTimeoutSeconds", 900L);
        ReflectionTestUtils.setField(task, "batchSize", 100);

        task.closeExpiredAndRetryCompensation();

        verify(mockedLifecycle).closeExpiredOrder(1L);
        verify(mockedLifecycle).closeExpiredOrder(2L);
    }

    private VoucherOrder order(Long id) {
        return new VoucherOrder()
                .setId(id)
                .setUserId(10L + id)
                .setVoucherId(100L)
                .setStatus(VoucherOrder.STATUS_PENDING_PAYMENT)
                .setCloseRetry(0);
    }
}
