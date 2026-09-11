package com.hmdp.service;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.monitor.SeckillEventLogger;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VoucherOrderLifecycleServiceTest {

    private static final long ORDER_ID = 123456789012345678L;
    private static final long USER_ID = 88L;
    private static final long VOUCHER_ID = 9L;

    @Mock
    private VoucherOrderMapper voucherOrderMapper;
    @Mock
    private SeckillVoucherMapper seckillVoucherMapper;
    @Mock
    private StringRedisTemplate stringRedisTemplate;
    @Mock
    private SeckillEventLogger seckillEventLogger;

    private VoucherOrderLifecycleService service;

    @BeforeEach
    void setUp() {
        service = new VoucherOrderLifecycleService();
        ReflectionTestUtils.setField(service, "voucherOrderMapper", voucherOrderMapper);
        ReflectionTestUtils.setField(service, "seckillVoucherMapper", seckillVoucherMapper);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", stringRedisTemplate);
        ReflectionTestUtils.setField(service, "seckillEventLogger", seckillEventLogger);
        ReflectionTestUtils.setField(service, "paymentTimeoutSeconds", 900L);
        UserDTO user = new UserDTO();
        user.setId(USER_ID);
        UserHolder.saveUser(user);
    }

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    @Test
    void paysPendingOrderAndCreatesRedemptionEligibility() {
        VoucherOrder pending = order(VoucherOrder.STATUS_PENDING_PAYMENT, LocalDateTime.now());
        // 支付 CAS 成功后会重新读库返回真值，第二次 selectById 必须是被更新后的已支付订单
        VoucherOrder paid = order(VoucherOrder.STATUS_PENDING_REDEMPTION, pending.getCreateTime()).setPayType(2);
        when(voucherOrderMapper.selectById(ORDER_ID)).thenReturn(pending, paid);
        when(voucherOrderMapper.update(isNull(), any(Wrapper.class))).thenReturn(1);

        Result result = service.payMyOrder(ORDER_ID, 2);

        assertTrue(result.getSuccess());
        VoucherOrder returned = (VoucherOrder) result.getData();
        assertEquals(VoucherOrder.STATUS_PENDING_REDEMPTION, returned.getStatus());
        assertEquals(2, returned.getPayType());
    }

    @Test
    void rejectsPaymentAfterDeadline() {
        when(voucherOrderMapper.selectById(ORDER_ID))
                .thenReturn(order(1, LocalDateTime.now().minusMinutes(16)));

        Result result = service.payMyOrder(ORDER_ID, 1);

        assertFalse(result.getSuccess());
        assertEquals("支付时限已过，订单正在关闭", result.getErrorMsg());
        // 截止时间校验放在 CAS 条件里，所以必须先尝试一次 UPDATE、再由 0 行结果判定超时
        verify(voucherOrderMapper).update(isNull(), any(Wrapper.class));
    }

    @Test
    void concurrentRepeatedPaymentReturnsIdempotentSuccess() {
        VoucherOrder pending = order(VoucherOrder.STATUS_PENDING_PAYMENT, LocalDateTime.now());
        VoucherOrder paid = order(VoucherOrder.STATUS_PENDING_REDEMPTION, pending.getCreateTime());
        when(voucherOrderMapper.selectById(ORDER_ID)).thenReturn(pending, paid);
        when(voucherOrderMapper.update(isNull(), any(Wrapper.class))).thenReturn(0);

        Result result = service.payMyOrder(ORDER_ID, 1);

        assertTrue(result.getSuccess());
        assertEquals(VoucherOrder.STATUS_PENDING_REDEMPTION,
                ((VoucherOrder) result.getData()).getStatus());
    }

    @Test
    void closeWinsPaymentRaceWithoutReportingPaymentSuccess() {
        VoucherOrder pending = order(VoucherOrder.STATUS_PENDING_PAYMENT, LocalDateTime.now());
        VoucherOrder closing = order(VoucherOrder.STATUS_CLOSING, pending.getCreateTime());
        when(voucherOrderMapper.selectById(ORDER_ID)).thenReturn(pending, closing);
        when(voucherOrderMapper.update(isNull(), any(Wrapper.class))).thenReturn(0);

        Result result = service.payMyOrder(ORDER_ID, 1);

        assertFalse(result.getSuccess());
        assertEquals("订单已由关单流程锁定，本次支付未生效", result.getErrorMsg());
    }

    @Test
    void cancelRestoresMysqlAndRedisExactlyOnce() {
        when(voucherOrderMapper.selectById(ORDER_ID))
                .thenReturn(order(1, LocalDateTime.now()), order(7, LocalDateTime.now()));
        when(voucherOrderMapper.update(isNull(), any(Wrapper.class))).thenReturn(1);
        when(seckillVoucherMapper.update(isNull(), any(Wrapper.class))).thenReturn(1);
        when(stringRedisTemplate.execute(
                any(DefaultRedisScript.class), anyList(), any(), any(), any())).thenReturn(1L);

        Result result = service.cancelMyOrder(ORDER_ID);

        assertTrue(result.getSuccess());
        verify(seckillVoucherMapper).update(isNull(), any(Wrapper.class));
        verify(stringRedisTemplate).execute(
                any(DefaultRedisScript.class), anyList(), any(), any(), any());
    }

    @Test
    void paidOrderCannotBeCancelled() {
        when(voucherOrderMapper.selectById(ORDER_ID))
                .thenReturn(order(VoucherOrder.STATUS_PENDING_REDEMPTION, LocalDateTime.now()));

        Result result = service.cancelMyOrder(ORDER_ID);

        assertFalse(result.getSuccess());
        assertEquals("只有待支付预约可以取消", result.getErrorMsg());
        verify(seckillVoucherMapper, never()).update(any(), any());
    }

    private VoucherOrder order(int status, LocalDateTime createTime) {
        return new VoucherOrder()
                .setId(ORDER_ID)
                .setUserId(USER_ID)
                .setVoucherId(VOUCHER_ID)
                .setStatus(status)
                .setCreateTime(createTime);
    }
}
