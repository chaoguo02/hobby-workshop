package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 秒杀本地消息表：lua 预扣成功后由请求线程落库，relay 定时投递到 Kafka。
 * 用于保证 Redis 预扣与 Kafka 投递之间的可靠衔接。
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("tb_seckill_message")
public class SeckillMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 已准入，待 relay 投递 Kafka */
    public static final int STATUS_READY = 0;
    /** relay 已抢占，正在投递（多实例防重复发送） */
    public static final int STATUS_PROCESSING = 1;
    /** 已投递到 Kafka（仅表示送达 broker，不代表订单已落库） */
    public static final int STATUS_SENT = 2;
    /** 订单已落库并扣减库存（与业务同事务提交，链路的终点） */
    public static final int STATUS_COMPLETED = 3;
    /**
     * 重投多次仍未完成：用于告警，并转入更长的慢速退避通道由 relay 继续自动重试。
     * 不是终态——人工重放只用于清零退避、加速处理，不作为唯一恢复手段。
     */
    public static final int STATUS_FAILED = 4;

    /**
     * 订单id，同时作为消息唯一标识
     */
    @TableId(value = "order_id", type = IdType.INPUT)
    private Long orderId;

    /**
     * 下单用户id
     */
    private Long userId;

    /**
     * 优惠券id
     */
    private Long voucherId;

    /**
     * 投递状态：0 READY，1 PROCESSING，2 SENT，3 COMPLETED，4 FAILED
     */
    private Integer status;

    /**
     * 投递重试次数
     */
    private Integer retry;

    /**
     * 下次可投递时间（退避用），null 表示立即可投
     */
    private LocalDateTime nextRetryTime;

    /**
     * 创建时间
     */
    private LocalDateTime createTime;

    /**
     * 更新时间
     */
    private LocalDateTime updateTime;
}
