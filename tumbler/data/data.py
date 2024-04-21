# coding=utf-8

from datetime import datetime, timedelta
import time
import traceback

from tumbler.object import BarData
from tumbler.constant import Interval, Exchange
from tumbler.function import get_format_lower_symbol, get_vt_key
from tumbler.service.log_service import log_service_manager
from tumbler.service import mongo_service_manager


class DataClient(object):
    def format_time_stamp_10(self, s):
        if len(str(int(s))) == 13:
            return int(float(s) / 1000.0)
        else:
            return int(s)

    def format_time_stamp_13(self, s):
        if len(str(int(s))) == 10:
            return int(s * 1000.0)
        else:
            return int(s)

    def make_request(self, symbol, interval, start_dt=None, end_dt=None, limit=1000):
        return {}

    @staticmethod
    def get_available_interval():
        return []

    def get_format_symbol(self, symbol):
        return symbol

    def get_format_period(self, period):
        return period

    def get_exchange(self):
        return ""

    def get_bars_to_pandas_data(self, symbols=[], period=Interval.DAY.value, start_datetime=datetime(2010, 1, 1),
                                end_datetime=datetime(2024, 12, 31)):
        bars = self.get_klines(symbols, period, start_datetime, end_datetime)
        return BarData.get_pandas_from_bars(bars)

    def get_klines(self, symbols, period, start_datetime=None, end_datetime=None):
        ret = []
        for symbol in symbols:
            bars = self.get_kline(
                symbol=symbol, period=period, start_datetime=start_datetime, end_datetime=end_datetime)
            ret.extend(bars)
        return ret

    def get_kline(self, symbol, period, start_datetime=None, end_datetime=None):
        ori_symbol = get_format_lower_symbol(symbol)
        symbol = self.get_format_symbol(symbol)
        period = self.get_format_period(period)
        exchange = self.get_exchange()
        ret_bars = []
        if isinstance(start_datetime, datetime):
            start_timestamp = int(time.mktime(start_datetime.timetuple()) * 1e3)
        else:
            start_timestamp = None

        try:
            bars = []
            if start_timestamp is not None:
                if end_datetime is None:
                    end_timestamp = int(time.mktime(datetime.now().timetuple()) * 1e3)
                else:
                    end_timestamp = int(time.mktime(end_datetime.timetuple()) * 1e3)

                while start_timestamp <= end_timestamp:
                    log_service_manager.write_log(start_timestamp, end_timestamp)
                    try:
                        log_service_manager.write_log('[get_kline] from {}~~{} download {} data'
                                                      .format(datetime.fromtimestamp(start_timestamp / 1e3),
                                                              datetime.fromtimestamp(end_timestamp / 1e3),
                                                              symbol))

                        bars_per_loop = self.make_request(symbol=symbol, interval=period,
                                                          start_dt=start_timestamp, end_dt=end_timestamp, limit=1000)
                        if len(bars_per_loop) == 0:
                            log_service_manager.write_log(u"bars' len is zero")
                            break

                        # 更新开始时间，为这次取得最后一个值
                        start_timestamp = bars_per_loop[-1][0]
                        if len(bars) > 0:
                            while bars[-1][0] >= bars_per_loop[0][0]:
                                if len(bars_per_loop) == 1:
                                    bars_per_loop = []
                                    start_timestamp = end_timestamp + 1
                                    break
                                bars_per_loop.pop(0)

                        # 追加bars
                        bars.extend(bars_per_loop)
                        if self.get_exchange() in [Exchange.HUOBI.value]:
                            log_service_manager.write_log(f"[get_kline] break exchange:{Exchange.HUOBI.value}")
                            break

                    except Exception as ex:
                        self.write_error(
                            u'Download data {} {} has error:{},{}'.format(symbol, period, str(ex),
                                                                          traceback.format_exc()))
                        break

            log_service_manager.write_log("[get_kline] DownloadData Finished!")
            for i, bar in enumerate(bars):
                try:
                    b = BarData()
                    b.symbol = ori_symbol
                    b.exchange = exchange
                    b.vt_symbol = get_vt_key(b.symbol, b.exchange)
                    b.open_price = float(bar[1])
                    b.high_price = float(bar[2])
                    b.low_price = float(bar[3])
                    b.close_price = float(bar[4])

                    b.datetime = datetime.fromtimestamp(bar[0] / 1e3)
                    b.date = b.datetime.strftime("%Y-%m-%d")
                    b.time = b.datetime.strftime("%H:%M:%S")

                    b.volume = float(bar[5])
                    b.interval = period

                    b.gateway_name = "DB"
                    ret_bars.append(b)
                except Exception as ex:
                    self.write_error(
                        'error when convert bar:{},ex:{},t:{}'.format(bar, str(ex), traceback.format_exc()))
            return ret_bars
        except Exception as ex:
            log_service_manager.write_log(
                'exception in get:{},{},{}'.format(symbol, str(ex), traceback.format_exc()))
            return ret_bars

    def download_save_mongodb(self, symbol, _start_datetime, _end_datetime, interval):
        n = 0
        start_datetime = _start_datetime
        end_datetime = _end_datetime
        while start_datetime < end_datetime:
            try:
                if interval in [Interval.DAY.value]:
                    tday_datetime = start_datetime + timedelta(days=1000)
                else:
                    tday_datetime = start_datetime + timedelta(days=5)
                ret = self.get_kline(symbol=symbol, period=interval,
                                     start_datetime=start_datetime, end_datetime=tday_datetime)
                n += len(ret)
                mongo_service_manager.save_bar_data(ret)

                start_datetime = tday_datetime
            except Exception as ex:
                print(ex)
                continue
        return n

    def write_error(self, msg):
        log_service_manager.write_log(msg)
