USE voucher_engine;

-- 1. 优惠券活动模板表
CREATE TABLE `tb_voucher` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `title` VARCHAR(128) NOT NULL COMMENT '券名称',
  `stock` INT NOT NULL COMMENT '剩余库存',
  `status` TINYINT NOT NULL DEFAULT 1 COMMENT '1-上架 2-下架',
  `begin_time` DATETIME NOT NULL COMMENT '抢购开始时间',
  `end_time` DATETIME NOT NULL COMMENT '抢购结束时间',
  `expire_seconds` INT NOT NULL DEFAULT 900 COMMENT '抢到后多少秒未核销自动退还(默认15分钟)',
  `version` INT NOT NULL DEFAULT 0 COMMENT '乐观锁版本号(第三关压测对比用)',
  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='优惠券模板表';

-- 2. 用户抢券订单表
CREATE TABLE `tb_voucher_order` (
  `id` BIGINT NOT NULL COMMENT '全局唯一订单ID(雪花算法/Redis生成，不用自增)',
  `user_id` BIGINT NOT NULL COMMENT '用户ID',
  `voucher_id` BIGINT NOT NULL COMMENT '优惠券ID',
  `status` TINYINT NOT NULL DEFAULT 0 COMMENT '状态机：0-待核销 1-锁定中 2-已核销 3-已过期退还',
  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '抢券时间',
  `use_time` DATETIME NULL COMMENT '核销时间',
  `refund_time` DATETIME NULL COMMENT '过期退还时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_voucher` (`user_id`, `voucher_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='优惠券订单表';

-- 3. MQ 消费幂等防重表
CREATE TABLE `tb_mq_consume_log` (
  `msg_id` VARCHAR(64) NOT NULL COMMENT 'MQ消息唯一ID',
  `biz_type` VARCHAR(32) NOT NULL COMMENT '业务类型',
  `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`msg_id`),
  -- 清理任务按 create_time 分批删除，没有这个索引每批都要全表扫描
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='MQ消费幂等记录表';

-- 初始化一条测试数据：100张晚高峰5折打车神券
INSERT INTO `tb_voucher` (`id`, `title`, `stock`, `status`, `begin_time`, `end_time`, `expire_seconds`)
VALUES (1, '晚高峰打车5折立减神券', 100, 1, '2026-01-01 00:00:00', '2026-12-31 23:59:59', 900);

CREATE TABLE `tb_local_message` (
    `id` BIGINT NOT NULL COMMENT '消息主键',
    `biz_type` VARCHAR(32) NOT NULL COMMENT '业务类型',
    `biz_id` VARCHAR(64) NOT NULL COMMENT '业务唯一id',
    `exchange` VARCHAR(128) NOT NULL COMMENT '目标交换机',
    `routing_key` VARCHAR(128) NOT NULL COMMENT '目标路由键',
    `payload` TEXT NOT NULL COMMENT '消息体内容',
    `trace_id` VARCHAR(64) DEFAULT NULL COMMENT '链路追踪id',
    `status` TINYINT NOT NULL DEFAULT 0 COMMENT'状态0-待投递 1-已投递 2-失败重试中',
    `retry_count` INT NOT NULL DEFAULT 0 COMMENT '已重试次数',
    `next_retry_time` DATETIME NOT NULL COMMENT '下次允许重试时间',
    `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_biz` (`biz_type`, `biz_id`),
    KEY `idx_status_next_retry` (`status`, `next_retry_time`),
    -- 清理任务（status IN (1,2) AND update_time < ?）和 status=2 告警扫描
    -- （status = 2 AND update_time > ?）都走这个联合索引，单列 update_time 覆盖不了
    KEY `idx_status_update_time` (`status`, `update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='本地消息表';
