# coding=utf-8
import os
from datetime import datetime, timedelta

import numpy as np

from tumbler.constant import Interval
from tumbler.object import BarData
from tumbler.service import log_service_manager
from tumbler.service.mysql_service import MysqlService

calendars_dict = {
    Interval.MINUTE.value: "min1",
    Interval.HOUR.value: "hour1",
    Interval.DAY.value: "day1"
}

'''
需要在qlib里，执行以下命令
python3 scripts/dump_bin.py dump_all --csv_path  ~/.qlib/csv_data/dc_data_min1 --freq min --qlib_dir ~/.qlib/qlib_data/dc_data --include_fields open,close,high,low,volume
python3 scripts/dump_bin.py dump_all --csv_path  ~/.qlib/csv_data/dc_data_day1 --freq day --qlib_dir ~/.qlib/qlib_data/dc_data --include_fields open,close,high,low,volume
python3 scripts/dump_bin.py dump_all --csv_path  ~/.qlib/csv_data/dc_data_hour1 --freq hour --qlib_dir ~/.qlib/qlib_data/dc_data --include_fields open,close,high,low,volume

或
bash dump_data.sh
'''


class QlibDcDataMaker(object):
    """
    数字货币 数据导成 qlib格式
    """

    def __init__(self, dir_path="/Users/szh/.qlib/csv_data/dc_data"):
        self._dir_path = dir_path
        if not os.path.exists(self._dir_path):
            os.mkdir(self._dir_path)
        self.mysql_service_manager = MysqlService()

    def dump_mysql_to_qlib_data(self, interval_arr):
        symbols = self.mysql_service_manager.get_mysql_distinct_symbol()
        for interval in interval_arr:
            log_service_manager.write_log("[dump_mysql_to_qlib_data] interval:{}".format(interval))
            n_dir = "{}_{}".format(self._dir_path, calendars_dict[interval])
            if not os.path.exists(n_dir):
                os.mkdir(n_dir)
            for symbol in symbols:
                log_service_manager.write_log("[dump_mysql_to_qlib_data] symbol:{}".format(symbol))
                bars = self.mysql_service_manager.get_bars(symbols=[symbol], period=interval,
                                                           start_dt="2010-01-01 00:00:00",
                                                           end_dt="2022-01-01 00:00:00",
                                                           sort_way="symbol")
                if bars:
                    filter_keys = set([])
                    f = open("{}/{}.csv".format(n_dir, symbol), "w")
                    f.write(BarData.get_column_line()+"\n")
                    for bar in bars:
                        bar.qlib_datetime_format(interval)
                        key = bar.get_key()
                        if key not in filter_keys:
                            f.write(bar.get_line()+"\n")
                            filter_keys.add(key)
                    f.close()

    @staticmethod
    def produce_period_sequences(_interval, start_time=datetime(2007, 1, 1), end_time=datetime(2030, 1, 1)):
        inc_period = {
            Interval.MINUTE.value: timedelta(minutes=1),
            Interval.HOUR.value: timedelta(hours=1),
            Interval.DAY.value: timedelta(days=1)
        }[_interval]
        index_dic = {}
        ret = []
        ts = start_time
        te = end_time
        i = 1
        while ts < te:
            v = str(ts)
            ret.append(v)
            index_dic[v] = i
            ts += inc_period
            i = i + 1
        return ret, index_dic

    def mkdir_calendars(self):
        calendar_dir = os.path.join(self._dir_path, "calendars")
        if not os.path.exists(calendar_dir):
            os.mkdir(calendar_dir)
        for interval, txt_name in calendars_dict.items():
            log_service_manager.write_log("[QlibDcDataMaker] now work {} {}".format(interval, txt_name))
            a_seq, ind_dic = QlibDcDataMaker.produce_period_sequences(interval)

            # 导出日期文件
            filepath = os.path.join(calendar_dir,  txt_name + ".txt")
            if not os.path.exists(filepath):
                np.array(a_seq).tofile(filepath, sep='\n', format='%s')


if __name__ == "__main__":
    dc_maker = QlibDcDataMaker()
    dc_maker.dump_mysql_to_qlib_data([Interval.DAY.value])
    #dc_maker.dump_mysql_to_qlib_data([Interval.HOUR.value, Interval.DAY.value])
    #dc_maker.dump_mysql_to_qlib_data([Interval.MINUTE.value, Interval.HOUR.value, Interval.DAY.value])
    #dc_maker.mkdir_calendars()
