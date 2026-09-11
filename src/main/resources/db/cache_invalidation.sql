-- 对已有 hmdp 数据库执行本文件；不要为了新增该表重新执行包含 DROP TABLE 的完整 hmdp.sql。
CREATE TABLE IF NOT EXISTS `tb_cache_invalidation`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `cache_key` varchar(255) NOT NULL COMMENT '待失效的缓存 key',
  `lock_key` varchar(255) NOT NULL COMMENT '与缓存重建共用的锁 key',
  `status` tinyint(1) NOT NULL DEFAULT 0 COMMENT '0待处理，1已完成',
  `retry` int(11) NOT NULL DEFAULT 0 COMMENT '重试次数',
  `next_retry_time` timestamp NULL DEFAULT NULL COMMENT '下次重试时间',
  `last_error` varchar(500) NULL DEFAULT NULL COMMENT '最近一次错误',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_status_retry`(`status`, `next_retry_time`, `create_time`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Compact;
