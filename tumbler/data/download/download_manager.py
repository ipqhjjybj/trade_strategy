# coding=utf-8

from time import sleep
from datetime import datetime, timedelta

from tumbler.constant import Interval
from tumbler.service.mysql_service import MysqlService
from tumbler.service import log_service_manager

from tumbler.record.client_quick_query import ClientPosPriceQuery
from tumbler.constant import Exchange
from tumbler.object import BarData


def get_all_symbols():
    binance_exchange_info = ClientPosPriceQuery.query_exchange_info(Exchange.BINANCE.value)
    huobiu_exchange_info = ClientPosPriceQuery.query_exchange_info(Exchange.HUOBIU.value)

    binance_symbols = [x.symbol for x in binance_exchange_info]
    huobiu_symbols = [x.symbol for x in huobiu_exchange_info]

    all_symbols = [(x,) for x in binance_symbols if x in huobiu_symbols]
    return all_symbols


def get_spot_symbols():
    binance_exchange_info = ClientPosPriceQuery.query_exchange_info(Exchange.BINANCE.value)
    # huobiu_exchange_info = ClientPosPriceQuery.query_exchange_info(Exchange.HUOBIU.value)
    huobi_exchange_info = ClientPosPriceQuery.query_exchange_info(Exchange.HUOBI.value)

    binance_symbols = [x.symbol for x in binance_exchange_info]
    huobi_symbols = [x.symbol for x in huobi_exchange_info]

    all_symbols = [x for x in binance_symbols if x in huobi_symbols]
    return all_symbols


def get_binance_symbols():
    binance_exchange_info = ClientPosPriceQuery.query_exchange_info(Exchange.BINANCE.value)
    return [x.symbol for x in binance_exchange_info]


class DownloadManager(object):
    def __init__(self, mq_manager, client):
        self._mq_manager = mq_manager
        self._client = client

    def get_kline_to_mysql(self, symbol, period, _start_datetime=None, _end_datetime=None):
        try:
            if _start_datetime:
                start_datetime = _start_datetime
            else:
                start_datetime = datetime(2017, 8, 1)

            if _end_datetime:
                end_datetime = _end_datetime
            else:
                end_datetime = datetime.now()

            log_service_manager.write_log("[get_kline_to_mysql] symbol:{} period:{} "
                                          " start_datetime:{} end_datetime:{}"
                                          .format(symbol, period, start_datetime, end_datetime))
            ret = self._client.get_kline(symbol=symbol, period=period,
                                         start_datetime=start_datetime, end_datetime=end_datetime)
            self._mq_manager.replace_bars(ret, symbol, period)
        except Exception as ex:
            log_service_manager.write_log(
                "[Error][get_kline_to_mysql] ex:{} symbol:{}, period:{}, start_datetime:{}, end_datetime:{}".
                    format(ex, symbol, period, _start_datetime, _end_datetime))

    def recovery_k_line(self, symbol, period, _start_datetime=None, _end_datetime=None):
        log_service_manager.write_log("[recovery_k_line] symbol:{} period:{}".format(symbol, period))
        table_name = MysqlService.get_kline_table(period)
        ret = self._mq_manager.get_all_datetime(symbol, table_name)
        ret.sort()

        ll = len(ret)
        if 0 == ll:
            if _start_datetime:
                start_datetime = _start_datetime
            else:
                start_datetime = datetime(2017, 8, 1)

            if _end_datetime:
                end_datetime = _end_datetime
            else:
                end_datetime = datetime.now()
            self.get_kline_to_mysql(symbol, period, start_datetime, end_datetime)
        else:
            add_time = None
            if period == Interval.MINUTE.value:
                add_time = timedelta(minutes=1)

            elif period == Interval.HOUR.value:
                add_time = timedelta(hours=1)

            elif period == Interval.DAY.value:
                add_time = timedelta(days=1)

            elif period == Interval.HOUR4.value:
                add_time = timedelta(hours=4)

            if add_time:
                for i in range(1, ll):
                    bef_time = ret[i - 1]
                    now_time = ret[i]
                    end_datetime = datetime.strptime(now_time, '%Y-%m-%d %H:%M:%S')
                    to_compare = datetime.strptime(bef_time, '%Y-%m-%d %H:%M:%S') + add_time
                    if end_datetime > to_compare:
                        log_service_manager.write_log(
                            "need recovery symbol:{} period:{} start:{} end:{}".format(
                                symbol, period, to_compare, end_datetime))
                        start_datetime = datetime.strptime(bef_time, '%Y-%m-%d %H:%M:%S')
                        self.get_kline_to_mysql(symbol, period, start_datetime, end_datetime)

                if datetime.strptime(ret[-1], '%Y-%m-%d %H:%M:%S') + add_time + timedelta(minutes=1) < datetime.now():
                    start_datetime = datetime.strptime(ret[-1], '%Y-%m-%d %H:%M:%S')
                    end_datetime = datetime.now()
                    log_service_manager.write_log(
                        "need recovery symbol:{} period:{} start:{} end:{}".format(
                            symbol, period, start_datetime, end_datetime))

                    self.get_kline_to_mysql(symbol, period, start_datetime, end_datetime)
            else:
                log_service_manager.write_log("[Error] symbol:{} period:{}".format(symbol, period))

    def check_k_line(self, symbol, period, _start_datetime=None, _end_datetime=None):
        '''
        检查数据 与 币安交易所数据是否一致
        '''
        log_service_manager.write_log(f"[check_k_line] symbol:{symbol},period:{period},start_datetime:{_start_datetime},end_datetime:{_end_datetime}")
        if _start_datetime:
            start_datetime = _start_datetime
        else:
            start_datetime = datetime(2017, 8, 1)
        if _end_datetime:
            end_datetime = _end_datetime
        else:
            end_datetime = datetime.now() + timedelta(hours=1)

        log_service_manager.write_log(f"[check_k_line] symbol:{symbol}, period:{period}, "
                                      f"start_datetime:{start_datetime}, end_datetime:{end_datetime}")
        bars_db_arr = self._mq_manager.get_bars(symbols=[symbol], period=period,
                                                start_datetime=start_datetime, end_datetime=end_datetime)
        bars_db_dic = BarData.change_from_bar_array_to_dict(bars_db_arr)
        bars_exchanges_arr = self._client.get_kline(symbol=symbol, period=period,
                                                    start_datetime=start_datetime, end_datetime=end_datetime)

        to_replace_bars = []
        for bar in bars_exchanges_arr:
            key = bar.get_key()
            if key in bars_db_dic.keys():
                bar_db = bars_db_dic[key]

                if not bar.same(bar_db):
                    to_replace_bars.append(bar)
            else:
                to_replace_bars.append(bar)

        if to_replace_bars:
            log_service_manager.write_log(f"[check_k_line] to_replace_bars len:{len(to_replace_bars)}")
            self._mq_manager.replace_bars(to_replace_bars, symbol, period)

    def delete_k_line(self, symbol, period, _start_datetime=None, _end_datetime=None):
        if _start_datetime:
            start_datetime = _start_datetime
        else:
            start_datetime = datetime(2017, 8, 1)
        if _end_datetime:
            end_datetime = _end_datetime
        else:
            end_datetime = datetime.now() + timedelta(hours=1)

        log_service_manager.write_log(f"[delete_k_line] symbol:{symbol}, period:{period}, "
                                      f"start_datetime:{start_datetime}, end_datetime:{end_datetime}")
        self._mq_manager.delete_bars(symbols=[symbol], period=period,
                                     start_datetime=start_datetime, end_datetime=end_datetime)

    def work_func(self, func, func_name, include_symbols=[], not_include_symbols=[], suffix="",
                  start_datetime=None, end_datetime=None, periods=[]):
        log_service_manager.write_log(f"work_func, func:{func_name}")

        if not suffix and not include_symbols and func_name in ["delete_all_klines"]:
            log_service_manager.write_log(f"[work_func] {func_name} suffix:{suffix}, "
                                          f"include_symbols:{include_symbols}, maybe error!")
            return

        if include_symbols:
            symbols = include_symbols
        else:
            symbols = get_all_symbols()
            symbols = [x[0] for x in symbols]
        symbols = [x for x in symbols if x.endswith(suffix)]
        log_service_manager.write_log("[work_func] symbols:{}".format(symbols))

        symbols.sort()
        log_service_manager.write_log("symbols:{}".format(symbols))
        periods.reverse()
        for period in periods:
            for symbol in symbols:
                if symbol not in not_include_symbols:
                    # func(symbol, period, start_datetime, end_datetime)
                    try:
                        func(symbol, period, start_datetime, end_datetime)
                    except Exception as ex:
                        log_service_manager.write_log("[{}] error:{}".format(func_name, ex))

    def recovery_all_kline(self, include_symbols=[], not_include_symbols=[], suffix="",
                           start_datetime=None, end_datetime=None,
                           periods=list(MysqlService.get_table_period().keys())):
        return self.work_func(self.recovery_k_line, "check_all_kline", include_symbols, not_include_symbols, suffix,
                              start_datetime, end_datetime, periods)

    def check_all_kline(self, include_symbols=[], not_include_symbols=[], suffix="",
                        start_datetime=None, end_datetime=None,
                        periods=list(MysqlService.get_table_period().keys())):
        return self.work_func(self.check_k_line, "check_all_kline", include_symbols, not_include_symbols, suffix,
                              start_datetime, end_datetime, periods)

    def delete_all_klines(self, include_symbols=[], not_include_symbols=[], suffix="",
                          start_datetime=None, end_datetime=None,
                          periods=list(MysqlService.get_table_period().keys())):
        return self.work_func(self.delete_k_line, "delete_all_klines", include_symbols, not_include_symbols, suffix,
                              start_datetime, end_datetime, periods)

    def loop_get_all_klines(self):
        while True:
            try:
                self.recovery_all_kline()
                sleep(3600)
            except Exception as ex:
                log_service_manager.write_log("Error in loop_get_all_klines:{}".format(ex))
