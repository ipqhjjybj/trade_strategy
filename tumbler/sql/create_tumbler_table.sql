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

 Date: 25/05/2020 17:04:37
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for open_orders
-- ----------------------------
DROP TABLE IF EXISTS `open_orders`;
CREATE TABLE `open_orders` (
  `symbol` varchar(40) DEFAULT NULL,
  `side` varchar(30) DEFAULT NULL,
  `order_id` varchar(30) DEFAULT NULL,
  `open_price` double(20,8) DEFAULT NULL,
  `deal_price` double(20,8) DEFAULT NULL,
  `amount` double(20,8) DEFAULT NULL,
  `filled_amount` double(20,8) DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `strategy_type` varchar(20) DEFAULT NULL,
  `client_id` varchar(20) DEFAULT NULL,
  `order_time` varchar(30) DEFAULT NULL,
  `update_time` varchar(30) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ----------------------------
-- Table structure for realtime_fund
-- ----------------------------
DROP TABLE IF EXISTS `realtime_fund`;
CREATE TABLE `realtime_fund` (
  `date` varchar(30) DEFAULT NULL,
  `accounts` varchar(255) DEFAULT NULL,
  `account_type` varchar(30) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ----------------------------
-- Table structure for traded_orders
-- ----------------------------
DROP TABLE IF EXISTS `traded_orders`;
CREATE TABLE `traded_orders` (
  `symbol` varchar(20) DEFAULT NULL,
  `side` varchar(10) DEFAULT NULL,
  `order_id` varchar(20) DEFAULT NULL,
  `open_price` double(20,10) DEFAULT NULL,
  `deal_price` double(20,10) DEFAULT NULL,
  `amount` double(20,10) DEFAULT NULL,
  `filled_amount` double(20,10) DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `strategy_type` varchar(30) DEFAULT NULL,
  `client_id` varchar(20) DEFAULT NULL,
  `order_time` varchar(30) DEFAULT NULL,
  `update_time` varchar(30) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

SET FOREIGN_KEY_CHECKS = 1;


-- ----------------------------
-- Table structure for transfer_info
-- ----------------------------
DROP TABLE IF EXISTS `transfer_info`;
CREATE TABLE `transfer_info` (
  `id` bigint(255) NOT NULL COMMENT '主键',
  `coin` varchar(60) DEFAULT NULL COMMENT '转账的币种',
  `from_address` varchar(255) DEFAULT NULL COMMENT '从哪个地址转',
  `to_address` varchar(255) DEFAULT NULL COMMENT '转到哪个地址',
  `from_wallet_name` varchar(255) DEFAULT NULL COMMENT '从哪里转的账户',
  `to_wallet_name` varchar(255) DEFAULT NULL COMMENT '到哪里的钱包名字',
  `amount` double(255,0) DEFAULT NULL COMMENT '转账的金额',
  `create_time` varchar(255) DEFAULT NULL COMMENT '转账的时间',
  `type` varchar(255) DEFAULT NULL COMMENT '转账类型(in表示wallet内部转账, 2表示)',
  `comment` varchar(255) DEFAULT NULL COMMENT '备注',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

SET FOREIGN_KEY_CHECKS = 1;
