-- 仅用于已有数据库的增量迁移；新库 init.sql 已包含这些结构。
-- 停止应用、备份后执行一次。此脚本不切换任何现有券的库存模式。
ALTER TABLE tb_voucher ADD COLUMN stock_bucket_count INT NOT NULL DEFAULT 0 AFTER stock;
ALTER TABLE tb_voucher_order ADD COLUMN stock_bucket_id INT NULL AFTER voucher_id;
CREATE TABLE `tb_voucher_stock_bucket` (
  `voucher_id` BIGINT NOT NULL,
  `bucket_id` INT NOT NULL,
  `stock` INT NOT NULL,
  PRIMARY KEY (`voucher_id`, `bucket_id`),
  CHECK (`stock` >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='优惠券分桶库存';
