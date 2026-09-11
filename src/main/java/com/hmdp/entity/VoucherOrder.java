package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * <p>
 * 
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("tb_voucher_order")
public class VoucherOrder implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int STATUS_PENDING_PAYMENT = 1;
    public static final int STATUS_PENDING_REDEMPTION = 2;
    public static final int STATUS_REDEEMED = 3;
    public static final int STATUS_CANCELLED = 4;
    public static final int STATUS_CLOSING = 7;

    /**
     * 主键
     */
    @TableId(value = "id", type = IdType.INPUT)
    @JsonSerialize(using = ToStringSerializer.class)
    private Long id;

    /**
     * 下单的用户id
     */
    private Long userId;

    /**
     * 购买的代金券id
     */
    private Long voucherId;

    /**
     * 支付方式 1：余额支付；2：支付宝；3：微信
     */
    private Integer payType;

    /**
     * 预约状态，1：待支付；2：待核销；3：已核销；4：已取消；5：退款中；6：已退款；7：关闭处理中
     */
    private Integer status;

    /**
     * 下单时间
     */
    private LocalDateTime createTime;

    /**
     * 支付时间
     */
    private LocalDateTime payTime;

    /**
     * 核销时间
     */
    private LocalDateTime useTime;

    /**
     * 退款时间
     */
    private LocalDateTime refundTime;

    /**
     * 更新时间
     */
    private LocalDateTime updateTime;

    /** 关单补偿重试次数。 */
    private Integer closeRetry;

    /** 下一次允许执行 Redis 关单补偿的时间。 */
    private LocalDateTime closeNextRetryTime;

    /** 最近一次关单补偿异常，便于监控和排障。 */
    private String closeLastError;

    /** USER_CANCEL 或 PAYMENT_TIMEOUT。 */
    private String closeReason;

    /**
     * 待支付订单的支付截止时间，仅用于接口展示，不落库。
     */
    @TableField(exist = false)
    private LocalDateTime paymentDeadline;


}
