-- 对已有 hmdp 数据库执行。脚本可重复执行；完整 hmdp.sql 已包含这些字段和索引。
DROP PROCEDURE IF EXISTS `migrate_voucher_order_close`;
DELIMITER //
CREATE PROCEDURE `migrate_voucher_order_close`()
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tb_voucher_order'
                   AND COLUMN_NAME = 'close_retry') THEN
    ALTER TABLE `tb_voucher_order`
      ADD COLUMN `close_retry` int(11) NOT NULL DEFAULT 0 COMMENT '关单补偿重试次数' AFTER `update_time`;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tb_voucher_order'
                   AND COLUMN_NAME = 'close_next_retry_time') THEN
    ALTER TABLE `tb_voucher_order`
      ADD COLUMN `close_next_retry_time` timestamp NULL DEFAULT NULL COMMENT '下次关单补偿时间' AFTER `close_retry`;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tb_voucher_order'
                   AND COLUMN_NAME = 'close_last_error') THEN
    ALTER TABLE `tb_voucher_order`
      ADD COLUMN `close_last_error` varchar(500) NULL DEFAULT NULL COMMENT '最近一次关单补偿异常' AFTER `close_next_retry_time`;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tb_voucher_order'
                   AND COLUMN_NAME = 'close_reason') THEN
    ALTER TABLE `tb_voucher_order`
      ADD COLUMN `close_reason` varchar(32) NULL DEFAULT NULL COMMENT 'USER_CANCEL 或 PAYMENT_TIMEOUT' AFTER `close_last_error`;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tb_voucher_order'
                   AND INDEX_NAME = 'idx_status_create_time') THEN
    ALTER TABLE `tb_voucher_order`
      ADD INDEX `idx_status_create_time` (`status`, `create_time`);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tb_voucher_order'
                   AND INDEX_NAME = 'idx_status_close_retry') THEN
    ALTER TABLE `tb_voucher_order`
      ADD INDEX `idx_status_close_retry` (`status`, `close_next_retry_time`, `update_time`);
  END IF;
END//
DELIMITER ;
CALL `migrate_voucher_order_close`();
DROP PROCEDURE `migrate_voucher_order_close`;
