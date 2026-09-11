package com.hmdp.service;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.impl.VoucherOrderServiceImpl_kafka;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VoucherOrderRedemptionTest {

    private static final long ORDER_ID = 123456789012345678L;

    @Mock
    private VoucherOrderMapper voucherOrderMapper;

    private VoucherOrderServiceImpl_kafka service;

    @BeforeEach
    void setUp() {
        service = new VoucherOrderServiceImpl_kafka();
        ReflectionTestUtils.setField(service, "baseMapper", voucherOrderMapper);
    }

    @Test
    void rejectsInvalidVerificationCode() {
        Result result = service.redeemWorkshopOrder("WS-not-a-number");

        assertFalse(result.getSuccess());
        assertEquals("核销码格式不正确", result.getErrorMsg());
        verify(voucherOrderMapper, never()).selectById(any());
    }

    @Test
    void rejectsPendingPaymentOrder() {
        when(voucherOrderMapper.selectById(ORDER_ID))
                .thenReturn(orderWithStatus(VoucherOrder.STATUS_PENDING_PAYMENT));

        Result result = service.redeemWorkshopOrder("WS-" + ORDER_ID);

        assertFalse(result.getSuccess());
        assertEquals("只有已确认、待核销的预约才能核销", result.getErrorMsg());
        verify(voucherOrderMapper, never()).update(any(), any());
    }

    @Test
    void rejectsAlreadyRedeemedOrder() {
        when(voucherOrderMapper.selectById(ORDER_ID)).thenReturn(orderWithStatus(VoucherOrder.STATUS_REDEEMED));

        Result result = service.redeemWorkshopOrder(String.valueOf(ORDER_ID));

        assertFalse(result.getSuccess());
        assertEquals("该预约已经核销，请勿重复操作", result.getErrorMsg());
        verify(voucherOrderMapper, never()).update(any(), any());
    }

    @Test
    void redeemsPaidOrderWithConditionalUpdate() {
        when(voucherOrderMapper.selectById(ORDER_ID))
                .thenReturn(orderWithStatus(VoucherOrder.STATUS_PENDING_REDEMPTION),
                        orderWithStatus(VoucherOrder.STATUS_REDEEMED));
        when(voucherOrderMapper.update(isNull(), any(Wrapper.class))).thenReturn(1);

        Result result = service.redeemWorkshopOrder("ws-" + ORDER_ID);

        assertTrue(result.getSuccess());
        assertEquals(VoucherOrder.STATUS_REDEEMED, ((VoucherOrder) result.getData()).getStatus());
        verify(voucherOrderMapper).update(isNull(), any(Wrapper.class));
    }

    @Test
    void reportsConcurrentStatusChange() {
        when(voucherOrderMapper.selectById(ORDER_ID))
                .thenReturn(orderWithStatus(VoucherOrder.STATUS_PENDING_REDEMPTION));
        when(voucherOrderMapper.update(isNull(), any(Wrapper.class))).thenReturn(0);

        Result result = service.redeemWorkshopOrder("WS-" + ORDER_ID);

        assertFalse(result.getSuccess());
        assertEquals("订单状态已变化，请刷新后重试", result.getErrorMsg());
    }

    private VoucherOrder orderWithStatus(int status) {
        return new VoucherOrder().setId(ORDER_ID).setStatus(status);
    }
}
