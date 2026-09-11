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
 * 秒杀链路时间线事件：append-only，只插不改。
 * 以 order_id 为 trace id，id 自增顺序即时间线顺序。
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("tb_seckill_message_event")
public class SeckillMessageEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    private Long orderId;

    private Long userId;

    private Long voucherId;

    /** 阶段码，见 SeckillEventLogger 的 STAGE_* 常量 */
    private String stage;

    /** INFO / WARN / ERROR */
    private String level;

    private String detail;

    private LocalDateTime createTime;
}
