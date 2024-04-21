/*
 Navicat Premium Data Transfer

 Source Server         : tumbler
 Source Server Type    : MySQL
 Source Server Version : 50717
 Source Host           : localhost:3306
 Source Schema         : tumbler

 Target Server Type    : MySQL
 Target Server Version : 50717
 File Encoding         : 65001

 Date: 01/08/2021 21:36:58
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for plate
-- ----------------------------
DROP TABLE IF EXISTS `plate`;
CREATE TABLE `plate` (
  `plate_code` varchar(40) NOT NULL COMMENT '板块编号',
  `datetime` varchar(16) NOT NULL COMMENT '板块日期',
  `symbol` varchar(30) NOT NULL COMMENT '品种',
  `weight` double(20,8) DEFAULT NULL COMMENT '权重',
  PRIMARY KEY (`plate_code`,`datetime`,`symbol`),
  KEY `plate_sele` (`plate_code`(10),`datetime`(15)) USING BTREE,
  KEY `plate_code` (`plate_code`(16)) USING BTREE,
  KEY `plate_datetime` (`plate_code`(16),`datetime`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

SET FOREIGN_KEY_CHECKS = 1;


--
-- Table structure for table `plate_kline_1day`
--

DROP TABLE IF EXISTS `plate_kline_1day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `plate_kline_1day` (
  `symbol` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '币种名',
  `datetime` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '时间',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '当前周期成交量',
  UNIQUE KEY `unique_1day` (`datetime`,`symbol`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;


