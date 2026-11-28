-- MySQL dump 10.13  Distrib 8.0.46, for Linux (x86_64)
--
-- Host: localhost    Database: voucher_engine
-- ------------------------------------------------------
-- Server version	8.0.46

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Current Database: `voucher_engine`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `voucher_engine` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;

USE `voucher_engine`;

--
-- Table structure for table `tb_local_message`
--

DROP TABLE IF EXISTS `tb_local_message`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `tb_local_message` (
  `id` bigint NOT NULL COMMENT '消息主键',
  `biz_type` varchar(32) NOT NULL COMMENT '业务类型',
  `biz_id` varchar(64) NOT NULL COMMENT '业务唯一id',
  `exchange` varchar(128) NOT NULL COMMENT '目标交换机',
  `routing_key` varchar(128) NOT NULL COMMENT '目标路由键',
  `payload` text NOT NULL COMMENT '消息体内容',
  `trace_id` varchar(64) DEFAULT NULL COMMENT '链路追踪ID',
  `status` tinyint NOT NULL DEFAULT '0' COMMENT '状态0-待投递 1-已投递 2-失败重试中',
  `retry_count` int NOT NULL DEFAULT '0' COMMENT '已重试次数',
  `next_retry_time` datetime NOT NULL COMMENT '下次允许重试时间',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_biz` (`biz_type`,`biz_id`),
  KEY `idx_status_next_retry` (`status`,`next_retry_time`),
  KEY `idx_status_update_time` (`status`,`update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='本地消息表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tb_local_message`
--

LOCK TABLES `tb_local_message` WRITE;
/*!40000 ALTER TABLE `tb_local_message` DISABLE KEYS */;
/*!40000 ALTER TABLE `tb_local_message` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tb_mq_consume_log`
--

DROP TABLE IF EXISTS `tb_mq_consume_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `tb_mq_consume_log` (
  `msg_id` varchar(64) NOT NULL COMMENT 'MQæ¶ˆæ¯å”¯ä¸€ID',
  `biz_type` varchar(32) NOT NULL COMMENT 'ä¸šåŠ¡ç±»åž‹',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`msg_id`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='MQæ¶ˆè´¹å¹‚ç­‰è®°å½•è¡¨';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tb_mq_consume_log`
--

LOCK TABLES `tb_mq_consume_log` WRITE;
/*!40000 ALTER TABLE `tb_mq_consume_log` DISABLE KEYS */;
/*!40000 ALTER TABLE `tb_mq_consume_log` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tb_voucher`
--

DROP TABLE IF EXISTS `tb_voucher`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `tb_voucher` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'ä¸»é”®ID',
  `title` varchar(128) NOT NULL COMMENT 'åˆ¸åç§°',
  `stock` int NOT NULL COMMENT 'å‰©ä½™åº“å­˜',
  `status` tinyint NOT NULL DEFAULT '1' COMMENT '1-ä¸Šæž¶ 2-ä¸‹æž¶',
  `begin_time` datetime NOT NULL COMMENT 'æŠ¢è´­å¼€å§‹æ—¶é—´',
  `end_time` datetime NOT NULL COMMENT 'æŠ¢è´­ç»“æŸæ—¶é—´',
  `expire_seconds` int NOT NULL DEFAULT '900' COMMENT 'æŠ¢åˆ°åŽå¤šå°‘ç§’æœªæ ¸é”€è‡ªåŠ¨é€€è¿˜(é»˜è®¤15åˆ†é’Ÿ)',
  `version` int NOT NULL DEFAULT '0' COMMENT 'ä¹è§‚é”ç‰ˆæœ¬å·(ç¬¬ä¸‰å…³åŽ‹æµ‹å¯¹æ¯”ç”¨)',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ä¼˜æƒ åˆ¸æ¨¡æ¿è¡¨';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tb_voucher`
--

LOCK TABLES `tb_voucher` WRITE;
/*!40000 ALTER TABLE `tb_voucher` DISABLE KEYS */;
INSERT INTO `tb_voucher` VALUES (1,'æ™šé«˜å³°æ‰“è½¦5æŠ˜ç«‹å‡ç¥žåˆ¸',100,1,'2020-01-01 00:00:00','2099-12-31 23:59:59',900,502,'2026-11-22 09:06:55','2026-11-28 11:57:49');
/*!40000 ALTER TABLE `tb_voucher` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tb_voucher_order`
--

DROP TABLE IF EXISTS `tb_voucher_order`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `tb_voucher_order` (
  `id` bigint NOT NULL COMMENT 'å…¨å±€å”¯ä¸€è®¢å•ID(é›ªèŠ±ç®—æ³•/Redisç”Ÿæˆï¼Œä¸ç”¨è‡ªå¢ž)',
  `user_id` bigint NOT NULL COMMENT 'ç”¨æˆ·ID',
  `voucher_id` bigint NOT NULL COMMENT 'ä¼˜æƒ åˆ¸ID',
  `status` tinyint NOT NULL DEFAULT '0' COMMENT 'çŠ¶æ€æœºï¼š0-å¾…æ ¸é”€ 1-é”å®šä¸­ 2-å·²æ ¸é”€ 3-å·²è¿‡æœŸé€€è¿˜',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'æŠ¢åˆ¸æ—¶é—´',
  `use_time` datetime DEFAULT NULL COMMENT 'æ ¸é”€æ—¶é—´',
  `refund_time` datetime DEFAULT NULL COMMENT 'è¿‡æœŸé€€è¿˜æ—¶é—´',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_voucher` (`user_id`,`voucher_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ä¼˜æƒ åˆ¸è®¢å•è¡¨';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tb_voucher_order`
--

LOCK TABLES `tb_voucher_order` WRITE;
/*!40000 ALTER TABLE `tb_voucher_order` DISABLE KEYS */;
/*!40000 ALTER TABLE `tb_voucher_order` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping routines for database 'voucher_engine'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-11-28 13:04:29
