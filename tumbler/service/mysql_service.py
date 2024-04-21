# coding=utf-8
from datetime import datetime
from collections import defaultdict
import pandas as pd

import json
import MySQLdb
from DBUtils.PooledDB import PooledDB

import tumbler.config as config
from tumbler.object import BarData, FactorData, FundamentalData
from tumbler.constant import Interval
from tumbler.service.log_service import log_service_manager

table_period = {
    Interval.MINUTE.value: "1min",
    Interval.HOUR.value: "1hour",
    Interval.HOUR4.value: "4hour",
    Interval.DAY.value: "1day",
}


class MysqlService(object):
    def __init__(self):
        self.pool = PooledDB(MySQLdb, 5, host=config.SETTINGS["mysql_host"], user=config.SETTINGS["mysql_user"],
                             passwd=config.SETTINGS["mysql_password"], db=config.SETTINGS["mysql_database"],
                             port=config.SETTINGS["mysql_port"])  # 5为连接池里的最少连接数

        self.max_update_nums = 5000

    def get_conn(self):
        return self.pool.connection()

    @staticmethod
    def get_sql_params(**kwargs):
        arr = []
        for k, v in kwargs.items():
            arr.append("{}='{}'".format(k, v))
        return ','.join(arr)

    @staticmethod
    def get_kline_table(period):
        return "`tumbler`.`kline_{}`".format(table_period[period])

    @staticmethod
    def get_factor_table(period):
        return "`tumbler`.`factor_{}`".format(table_period[period])

    @staticmethod
    def get_symbol_table():
        return "`tumbler`.`symbols`"

    @staticmethod
    def get_fundamental_table():
        return "`tumbler`.`symbol_fundamental`"

    @staticmethod
    def get_table_period():
        return table_period

    @staticmethod
    def get_array_to_string(arr):
        return ','.join(["'" + v + "'" for v in arr])

    @staticmethod
    def filter_symbols(symbols, suffix="_usdt"):
        return [symbol for symbol in symbols if symbol.endswith(suffix)]

    @staticmethod
    def get_mysql_service():
        global mysql_service_manager
        if not mysql_service_manager:
            mysql_service_manager = MysqlService()
        return mysql_service_manager

    def update_asset_info(self, asset, **kwargs):
        flag = True
        conn = self.get_conn()
        sqll = f"select id, tags from `tumbler`.`symbol_fundamental` where `asset`='{asset}'"
        cur = conn.cursor()
        cur.execute(sqll)
        myresult = cur.fetchall()
        sqll = ""
        try:
            if len(myresult) > 0:
                Id, tags = myresult[0]
                if tags:
                    tags = json.loads(tags)
                else:
                    tags = []
                if "tags" in kwargs.keys():
                    tags.extend(kwargs["tags"])
                    tags = list(set(tags))
                    kwargs["tags"] = tags

                kwargs["tags"] = json.dumps(kwargs["tags"])
                set_params = self.get_sql_params(**kwargs)

                sqll = f"update `tumbler`.`symbol_fundamental` set {set_params} where `id`='{Id}'"
                cur.execute(sqll)
                conn.commit()
            else:
                name = kwargs.get("name", "")
                chain = kwargs.get("chain", "")
                exchange = kwargs.get("exchange", "")
                max_supply = kwargs.get("max_supply", 0)
                tags = json.dumps(kwargs.get("tags", []))
                create_date = datetime.now().strftime("%Y-%m-%d")
                sqll = f"insert into `tumbler`.`symbol_fundamental`" \
                       f"(id, asset, name, chain, exchange, max_supply, tags, create_date)" \
                       f" values (NULL, '{asset}', '{name}', '{chain}', '{exchange}', " \
                       f" '{max_supply}', '{tags}', '{create_date}')"
                cur.execute(sqll)
                conn.commit()
        except Exception as ex:
            flag = False
            log_service_manager.write_log(f"[update_asset_info] ex:{ex} sqll:{sqll}")
        cur.close()
        conn.close()
        return flag

    def get_mysql_distinct_symbol(self, table='kline_1hour'):
        ret = []
        if "`tumbler`." in table:
            sqll = "select distinct symbol from {} order by symbol asc".format(table)
        else:
            sqll = "select distinct symbol from `tumbler`.`{}` order by symbol asc".format(table)
        conn = self.get_conn()
        cur = conn.cursor()

        cur.execute(sqll)
        myresult = cur.fetchall()
        for x, in myresult:
            ret.append(x)

        cur.close()
        conn.close()
        return ret

    def get_all_datetime(self, symbol, table_name):
        ret = []
        sqll = "select datetime from {} where symbol='{}' order by datetime asc".format(table_name, symbol)
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sqll)
        my_result = cur.fetchall()
        for x, in my_result:
            ret.append(x)
        cur.close()
        conn.close()
        ret.sort()
        return ret

    def get_distinct_tags(self):
        ret = []
        sqll = "select distinct(`tags`) from `tumbler`.`symbol_fundamental`"
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sqll)
        my_result = cur.fetchall()
        for tags, in my_result:
            tags = json.loads(tags)
            ret.extend(tags)
        cur.close()
        conn.close()
        ret = list(set(ret))
        ret.sort()
        return ret

    def get_tag_asset_dic(self):
        ret_dic = defaultdict(list)
        sqll = "select `asset`, `tags` from `tumbler`.`symbol_fundamental`"
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sqll)
        my_result = cur.fetchall()
        for asset, tags in my_result:
            tags = json.loads(tags)
            for tag in tags:
                ret_dic[tag].append(asset)
        cur.close()
        conn.close()
        return ret_dic

    def get_distinct_datetime(self, table_name):
        ret = []
        sqll = f"select distinct(`datetime`) from {table_name}"
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sqll)
        my_result = cur.fetchall()
        for x, in my_result:
            ret.append(x)
        cur.close()
        conn.close()
        ret.sort()
        return ret

    def delete_distinct_datetime(self, table_name, str_datetime):
        sqll = f"delete from {table_name} where datetime = '{str_datetime}';"
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sqll)
        cur.close()
        conn.close()

    def select_coins(self):
        conn = self.get_conn()
        ret = []
        cur = conn.cursor()

        sqll = "select `id`,`coin`,`name`,`circulating_supply`,`max_supply` from `coin`.`coin_fundamental`"
        cur.execute(sqll)

        my_result = cur.fetchall()
        for x in my_result:
            ret.append(x)

        cur.close()
        conn.close()

        ret.sort()
        return ret

    def get_all_base_symbol(self, end_base="_usdt"):
        conn = self.get_conn()
        sqll = "select distinct symbol from {}".format(MysqlService.get_symbol_table())
        ret = set([])
        cur = conn.cursor()
        cur.execute(sqll)
        my_result = cur.fetchall()
        for symbol, in my_result:
            if symbol.endswith(end_base):
                ret.add(symbol)
        cur.close()
        conn.close()
        return list(ret)

    def get_fundamentals(self):
        conn = self.get_conn()
        sqll = "select asset, name, chain, exchange, max_supply, tags from {} where id > '0'"\
            .format(MysqlService.get_fundamental_table())
        ret = []
        cur = conn.cursor()
        cur.execute(sqll)
        my_result = cur.fetchall()
        for arr in my_result:
            ret.append(FundamentalData.init_from_mysql_db(arr))
        cur.close()
        conn.close()
        return ret

    def get_bars(self, symbols=[], period=Interval.DAY.value, start_datetime=datetime(2017, 1, 1),
                 end_datetime=datetime(2022, 12, 31), sort_way="symbol"):
        start_dt_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
        end_dt_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

        conn = self.get_conn()
        sqll = "select * from {} where datetime >= '{}' and datetime <= '{}'".format(
            MysqlService.get_kline_table(period), start_dt_str, end_dt_str)

        if len(symbols) > 0:
            sqll = sqll + " and symbol in ({})".format(MysqlService.get_array_to_string(symbols))
        if sort_way == "symbol":
            sqll = sqll + " order by symbol asc, datetime asc"
        else:
            sqll = sqll + " order by datetime asc, symbol asc"

        ret = []
        cur = conn.cursor()
        cur.execute(sqll)
        my_result = cur.fetchall()
        for arr in my_result:
            ret.append(BarData.init_from_mysql_db(arr))
        cur.close()
        conn.close()
        return ret

    def replace_factor(self, ret, symbol, period, factor_code):
        if ret:
            start_datetime = ret[0][2]
            end_datetime = ret[-1][2]
            log_service_manager.write_log(f"[replace_factor] {symbol} {factor_code} {period} "
                                          f"{start_datetime} {end_datetime} {len(ret)}")
            table_name = MysqlService.get_factor_table(period)
            conn = self.get_conn()
            cur = conn.cursor()
            ll = len(ret)
            ii = 0
            while ii < ll:
                tmp_rets = ret[ii: ii + self.max_update_nums]
                all_datetime_strs = [x[2] for x in tmp_rets]
                ii = ii + self.max_update_nums

                if len(all_datetime_strs) > 0:
                    all_datetime_strs = ["'" + x + "'" for x in all_datetime_strs]
                    sqll = "delete from {} where datetime in ({}) and symbol='{}' and factor_code='{}'". \
                        format(table_name, ','.join(all_datetime_strs), symbol, factor_code)
                    cur.execute(sqll)
                if len(tmp_rets) > 0:
                    sqll = "insert into {}(factor_code,symbol,datetime,val) values(%s,%s,%s,%s)" \
                        .format(table_name)
                    cur.executemany(sqll, tmp_rets)
                conn.commit()

            cur.close()
            conn.close()

    def replace_factor_from_pd(self, df, period):
        df = df.set_index(["symbol", "datetime"])
        for col in df.columns:
            if col not in BarData.get_columns() and col not in ["symbol", "exchange"]:
                series = df[col].dropna()
                ret = FactorData.get_data_from_series(col, series)
                diff_dic = FactorData.get_diff_symbol_from_ret(ret)
                for symbol in list(diff_dic.keys()):
                    self.replace_factor(diff_dic[symbol], symbol, period, col)

    def replace_bars(self, ret, symbol, period):
        if ret:
            start_time = ret[0].datetime
            end_datetime = ret[-1].datetime
            log_service_manager.write_log(f"[replace_bars] {symbol} {period} {start_time} {end_datetime} {len(ret)}")

            table_name = MysqlService.get_kline_table(period)
            conn = self.get_conn()
            cur = conn.cursor()
            new_ret = BarData.from_bar_array_to_mysql_data(ret)
            ll = len(new_ret)
            ii = 0
            while ii < ll:
                tmp_rets = new_ret[ii: ii + self.max_update_nums]
                all_datetime_strs = [x[1] for x in tmp_rets]
                ii = ii + self.max_update_nums

                if len(all_datetime_strs) > 0:
                    all_datetime_strs = ["'" + x + "'" for x in all_datetime_strs]
                    sqll = "delete from {} where datetime in ({}) and symbol='{}'". \
                        format(table_name, ','.join(all_datetime_strs), symbol)
                    cur.execute(sqll)
                if len(tmp_rets) > 0:
                    sqll = "insert into {}(symbol,datetime,open,high,low,close,volume) values(%s,%s,%s,%s,%s,%s,%s)" \
                        .format(table_name)
                    cur.executemany(sqll, tmp_rets)
                conn.commit()

            cur.close()
            conn.close()

    def insert_bars(self, ret, period):
        if ret:
            start_time = ret[0].datetime
            end_datetime = ret[-1].datetime
            symbol = ret[0].symbol
            log_service_manager.write_log(f"[insert_bars] {symbol} {period} {start_time} {end_datetime} {len(ret)}")

            new_ret = BarData.from_bar_array_to_mysql_data(ret)
            conn = self.get_conn()
            cur = conn.cursor()

            ll = len(ret)
            ii = 0

            while ii < ll:
                tmp_rets = new_ret[ii: ii + self.max_update_nums]
                ii = ii + self.max_update_nums

                table_name = MysqlService.get_kline_table(period)
                sqll = "insert into {}(symbol,datetime,open,high,low,close,volume) values(%s,%s,%s,%s,%s,%s,%s)" \
                    .format(table_name)
                try:
                    cur.executemany(sqll, tmp_rets)
                    conn.commit()
                except Exception as ex:
                    log_service_manager.write_log(f"[insert_bars] insert error, insert one by one! ex:{ex}")
                    for ret in tmp_rets:
                        try:
                            cur.executemany(sqll, [ret])
                            conn.commit()
                        except Exception as ex:
                            log_service_manager.write_log(f"[insert_bars] insert one by one error, ex:{ex}")

            cur.close()
            conn.close()

    def delete_bars(self, symbols=[], period=Interval.DAY.value, start_datetime=datetime(2020, 1, 1),
                    end_datetime=datetime(2022, 12, 31)):
        start_dt_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
        end_dt_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

        conn = self.get_conn()
        cur = conn.cursor()
        for symbol in symbols:
            sqll = "delete from {} where symbol='{}' and datetime >= '{}' and datetime <= '{}'".format(
                MysqlService.get_kline_table(period), symbol, start_dt_str, end_dt_str)
            cur.execute(sqll)
            conn.commit()

            log_service_manager.write_log(f"{symbol} {period} {cur.rowcount} records deleted!")

        cur.close()
        conn.close()
        return True

    def get_bars_to_pandas_data(self, symbols=[], period=Interval.DAY.value, start_datetime=datetime(2010, 1, 1),
                                end_datetime=datetime(2024, 12, 31), sort_way="symbol"):
        bars = self.get_bars(symbols, period, start_datetime, end_datetime, sort_way=sort_way)
        return BarData.get_pandas_from_bars(bars)

    def get_factors(self, factor_codes, interval, start_dt, end_dt, sort_way="symbol"):
        if isinstance(factor_codes, str):
            factor_codes = [factor_codes]
        factor_str = ','.join([f"'{factor_code}'" for factor_code in factor_codes])
        conn = self.get_conn()
        sqll = "select `factor_code`, `symbol`, `datetime`, `val` from {} " \
               "where factor_code in({}) and datetime >= '{}' and datetime <= '{}'" \
            .format(MysqlService.get_factor_table(interval), factor_str, start_dt, end_dt)

        print(sqll)
        if sort_way == "symbol":
            sqll = sqll + " order by symbol asc, datetime asc"
        else:
            sqll = sqll + " order by datetime asc, symbol asc"

        ret = []
        cur = conn.cursor()
        cur.execute(sqll)
        my_result = cur.fetchall()
        for arr in my_result:
            ret.append(FactorData.init_from_mysql_db(arr))
        cur.close()
        conn.close()
        return ret


mysql_service_manager = None
# mysql_service_manager = MysqlService()
