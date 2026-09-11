package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

/** 数据库更新事务内记录的缓存失效事件。 */
@Data
@Accessors(chain = true)
@TableName("tb_cache_invalidation")
public class CacheInvalidationEvent implements Serializable {

    public static final int STATUS_PENDING = 0;
    public static final int STATUS_COMPLETED = 1;

    @TableId(type = IdType.AUTO)
    private Long id;
    private String cacheKey;
    private String lockKey;
    private Integer status;
    private Integer retry;
    private LocalDateTime nextRetryTime;
    private String lastError;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
