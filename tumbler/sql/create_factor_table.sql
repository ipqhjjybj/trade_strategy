
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for factor
-- ----------------------------
DROP TABLE IF EXISTS `factor`;
CREATE TABLE `factor` (
  `id` bigint(20) NOT NULL COMMENT '因子ID',
  `code` varchar(50) DEFAULT NULL COMMENT '因子英文编号',
  `name` varchar(255) DEFAULT NULL COMMENT '因子中文名',
  `period` varchar(30) DEFAULT NULL COMMENT '周期',
  `type` varchar(50) DEFAULT NULL COMMENT '因子大的类别',
  `py_content` text COMMENT '因子计算公式,py大',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

SET FOREIGN_KEY_CHECKS = 1;
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for day_rate_income
-- ----------------------------
DROP TABLE IF EXISTS `day_rate_income`;
CREATE TABLE `day_rate_income` (
  `id` bigint(20) NOT NULL,
  `factor_id` bigint(20) NOT NULL COMMENT '因子ID',
  `rate` double(30,12) DEFAULT NULL COMMENT '收益率',
  `ic` double(30,12) DEFAULT NULL COMMENT 'ic值',
  `ir` double(30,12) DEFAULT NULL COMMENT 'ir值',
  `detail` varbinary(255) DEFAULT NULL COMMENT '细节描述',
  `date` varbinary(20) DEFAULT NULL COMMENT '某日的因子值',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

SET FOREIGN_KEY_CHECKS = 1;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for week_rate_income
-- ----------------------------
DROP TABLE IF EXISTS `week_rate_income`;
CREATE TABLE `week_rate_income` (
  `id` bigint(20) NOT NULL,
  `factor_id` bigint(20) NOT NULL COMMENT '因子ID',
  `rate` double(30,12) DEFAULT NULL COMMENT '收益率',
  `ic` double(30,12) DEFAULT NULL COMMENT 'ic值',
  `ir` double(30,12) DEFAULT NULL COMMENT 'ir值',
  `detail` varbinary(255) DEFAULT NULL COMMENT '细节描述',
  `date` varbinary(20) DEFAULT NULL COMMENT '某日的因子值',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

SET FOREIGN_KEY_CHECKS = 1;



-- ----------------------------
-- Table structure for factor_1day
-- ----------------------------
DROP TABLE IF EXISTS `factor_1day`;
CREATE TABLE `factor_1day` (
  `factor_code` varchar(50) NOT NULL COMMENT '因子名字',
  `symbol` varchar(45) NOT NULL COMMENT '交易对',
  `datetime` varchar(20) NOT NULL COMMENT '因子日期',
  `val` double(50,30) NOT NULL COMMENT '因子值'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

SET FOREIGN_KEY_CHECKS = 1;
