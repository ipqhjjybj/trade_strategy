-- MySQL dump 10.13  Distrib 5.7.17, for Win64 (x86_64)
--
-- Host: localhost    Database: coin
-- ------------------------------------------------------
-- Server version	5.7.14

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `coin_fundamental`
--

DROP TABLE IF EXISTS `symbol_fundamental`;
CREATE TABLE `symbol_fundamental` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '币种ID',
  `asset` varchar(45) COLLATE utf8_bin DEFAULT NULL COMMENT '币种',
  `name` varchar(60) COLLATE utf8_bin DEFAULT NULL COMMENT '币种名',
  `chain` varchar(45) COLLATE utf8_bin DEFAULT NULL COMMENT '属于哪条链',
  `exchange` varchar(45) COLLATE utf8_bin DEFAULT NULL COMMENT '上币的交易所',
  `max_supply` double(45,0) DEFAULT NULL COMMENT '最大供应量',
  `tags` text COLLATE utf8_bin COMMENT '标签',
  `create_date` varchar(45) COLLATE utf8_bin DEFAULT NULL COMMENT '发行日期',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=4721 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `symbol_fundamental`
--

LOCK TABLES `symbol_fundamental` WRITE;
/*!40000 ALTER TABLE `symbol_fundamental` DISABLE KEYS */;
/*!40000 ALTER TABLE `symbol_fundamental` ENABLE KEYS */;
UNLOCK TABLES;


-- ----------------------------
-- Table structure for symbol_fundamental_daily
-- ----------------------------
DROP TABLE IF EXISTS `symbol_fundamental_daily`;
CREATE TABLE `symbol_fundamental_daily` (
  `id` int(20) unsigned zerofill NOT NULL AUTO_INCREMENT COMMENT '主键',
  `asset` varchar(20) NOT NULL COMMENT '编号名',
  `datetime` varchar(15) NOT NULL COMMENT '日期',
  `circulation` double(30,16) DEFAULT NULL COMMENT '流通量',
  `circulation_market_value` double(30,16) DEFAULT NULL COMMENT '流通市值',
  PRIMARY KEY (`id`),
  KEY `daily_fund_symbol` (`symbol`(14)) USING BTREE,
  KEY `daily_fund_datetime` (`datetime`(14)) USING BTREE,
  KEY `daily_fund_datetime_symbol` (`symbol`(14),`datetime`(14)) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

SET FOREIGN_KEY_CHECKS = 1;


--
-- Table structure for table `kline_15min`
--

DROP TABLE IF EXISTS `kline_15min`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kline_15min` (
  `symbol` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '币种名',
  `datetime` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '时间',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '当前周期成交量',
  UNIQUE KEY `unique_15min` (`datetime`,`symbol`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `kline_15min`
--

LOCK TABLES `kline_15min` WRITE;
/*!40000 ALTER TABLE `kline_15min` DISABLE KEYS */;
/*!40000 ALTER TABLE `kline_15min` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `kline_1day`
--

DROP TABLE IF EXISTS `kline_1day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kline_1day` (
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

--
-- Dumping data for table `kline_1day`
--

LOCK TABLES `kline_1day` WRITE;
/*!40000 ALTER TABLE `kline_1day` DISABLE KEYS */;
/*!40000 ALTER TABLE `kline_1day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `kline_1hour`
--

DROP TABLE IF EXISTS `kline_1hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kline_1hour` (
  `symbol` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '币种名',
  `datetime` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '时间',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '当前周期成交量',
  UNIQUE KEY `unique_1hour` (`datetime`,`symbol`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `kline_1hour`
--

LOCK TABLES `kline_1hour` WRITE;
/*!40000 ALTER TABLE `kline_1hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `kline_1hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `kline_1min`
--

DROP TABLE IF EXISTS `kline_1min`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kline_1min` (
  `symbol` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '币种名',
  `datetime` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '时间',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '当前周期成交量',
  UNIQUE KEY `unique_1min` (`datetime`,`symbol`),
  KEY `symbol_1min` (`symbol`) USING BTREE,
  KEY `datetime_1min` (`datetime`) USING BTREE
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `kline_1min`
--

LOCK TABLES `kline_1min` WRITE;
/*!40000 ALTER TABLE `kline_1min` DISABLE KEYS */;
/*!40000 ALTER TABLE `kline_1min` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `kline_1mon`
--

DROP TABLE IF EXISTS `kline_1mon`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kline_1mon` (
  `symbol` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '币种名',
  `datetime` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '时间',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '当前周期成交量',
  UNIQUE KEY `unique_1mon` (`datetime`,`symbol`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `kline_1mon`
--

LOCK TABLES `kline_1mon` WRITE;
/*!40000 ALTER TABLE `kline_1mon` DISABLE KEYS */;
/*!40000 ALTER TABLE `kline_1mon` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `kline_1week`
--

DROP TABLE IF EXISTS `kline_1week`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kline_1week` (
  `symbol` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '币种名',
  `datetime` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '时间',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '当前周期成交量',
  UNIQUE KEY `unique_1week` (`datetime`,`symbol`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `kline_1week`
--

LOCK TABLES `kline_1week` WRITE;
/*!40000 ALTER TABLE `kline_1week` DISABLE KEYS */;
/*!40000 ALTER TABLE `kline_1week` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `kline_1year`
--

DROP TABLE IF EXISTS `kline_1year`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kline_1year` (
  `symbol` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '币种名',
  `datetime` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '时间',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '当前周期成交量',
  UNIQUE KEY `unique_1year` (`datetime`,`symbol`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `kline_1year`
--

LOCK TABLES `kline_1year` WRITE;
/*!40000 ALTER TABLE `kline_1year` DISABLE KEYS */;
/*!40000 ALTER TABLE `kline_1year` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `kline_30min`
--

DROP TABLE IF EXISTS `kline_30min`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kline_30min` (
  `symbol` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '币种名',
  `datetime` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '时间',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '当前周期成交量',
  UNIQUE KEY `unique_30min` (`datetime`,`symbol`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `kline_30min`
--

LOCK TABLES `kline_30min` WRITE;
/*!40000 ALTER TABLE `kline_30min` DISABLE KEYS */;
/*!40000 ALTER TABLE `kline_30min` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `kline_4hour`
--

DROP TABLE IF EXISTS `kline_4hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kline_4hour` (
  `symbol` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '币种名',
  `datetime` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '时间',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '当前周期成交量',
  UNIQUE KEY `unique_4hour` (`datetime`,`symbol`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `kline_4hour`
--

LOCK TABLES `kline_4hour` WRITE;
/*!40000 ALTER TABLE `kline_4hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `kline_4hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `kline_5min`
--

DROP TABLE IF EXISTS `kline_5min`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kline_5min` (
  `symbol` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '币种名',
  `datetime` varchar(45) COLLATE utf8_bin NOT NULL COMMENT '时间',
  `open` double DEFAULT NULL COMMENT '开盘价',
  `high` double DEFAULT NULL COMMENT '最高价',
  `low` double DEFAULT NULL COMMENT '最低价',
  `close` double DEFAULT NULL COMMENT '收盘价',
  `volume` double DEFAULT NULL COMMENT '当前周期成交量',
  UNIQUE KEY `unique_5min` (`datetime`,`symbol`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `kline_5min`
--

LOCK TABLES `kline_5min` WRITE;
/*!40000 ALTER TABLE `kline_5min` DISABLE KEYS */;
/*!40000 ALTER TABLE `kline_5min` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;


