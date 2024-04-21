# coding=utf-8

from datetime import datetime, timedelta
import traceback
import requests

from tumbler.object import BarData
from tumbler.constant import Exchange, Interval
from tumbler.service.log_service import log_service_manager

BITMEX_PERIOD_MAPPING = {Interval.MINUTE.value: '1m', Interval.HOUR.value: '1h', Interval.DAY.value: '1d'}


class BitmexClient(object):

    def get_bitmex_datetime(self, t_datetime):
        return (t_datetime - timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    def get_datetime_from_bitmex_time(self, str_datetime):
        return datetime.strptime(str_datetime, '%Y-%m-%dT%H:%M:%S.%fZ') + timedelta(hours=7)

    def make_request(self, symbol, interval, start_dt=None, end_dt=None, limit=500):
        try:
            url = "https://www.bitmex.com" + "/api/v1/trade/bucketed"
            params = {"symbol": symbol, "binSize": interval, "partial": "false", "count": limit, "reverse": "false",
                      "startTime": self.get_bitmex_datetime(start_dt), "endTime": self.get_bitmex_datetime(end_dt)}
            response = requests.request("GET", url, params=params)

            data = response.json()
            return data
        except Exception as ex:
            log_service_manager.write_log(ex)
            return self.make_request(symbol, interval, start_dt, end_dt, limit)

    def get_kline(self, symbol, interval, start_dt=None, end_dt=None):
        bitmex_symbol = symbol.upper()
        bitmex_period = BITMEX_PERIOD_MAPPING.get(interval)

        ret_bars = []
        try:
            bars = []
            if start_dt is not None:
                if end_dt is None:
                    end_dt = datetime.now()

                while start_dt <= end_dt:
                    log_service_manager.write_log(u'from {} ~ {} download {} data!'.
                                                  format(start_dt.strftime('%Y-%m-%d %H:%M:%S.%f'),
                                                         end_dt.strftime('%Y-%m-%d %H:%M:%S.%f'), symbol))

                    try:
                        bars_per_loop = self.make_request(symbol=bitmex_symbol, interval=bitmex_period,
                                                          start_dt=start_dt, end_dt=end_dt, limit=500)

                        if len(bars_per_loop) == 0:
                            log_service_manager.write_log(u"bars' len is zero")
                            break

                        log_service_manager.write_log("bars_per_loop:{}".format(bars_per_loop))
                        # 更新开始时间，为这次取得最后一个值

                        new_dic = []
                        start_dt = self.get_datetime_from_bitmex_time(bars_per_loop[-1]["timestamp"])
                        for dic in bars_per_loop:
                            if len(bars) == 0 or self.get_datetime_from_bitmex_time(
                                    bars[-1]["timestamp"]) < self.get_datetime_from_bitmex_time(dic["timestamp"]):
                                new_dic.append(dic)

                        bars.extend(new_dic)

                        start_dt = start_dt + timedelta(minutes=1)

                        log_service_manager.write_log(str(start_dt))

                    except Exception as ex:
                        self.write_error(
                            u'Download data {} {} has error:{},{}'.format(bitmex_symbol, bitmex_period, str(ex),
                                                                          traceback.format_exc()))
                        break

            log_service_manager.write_log("DownloadData Finished!")
            for i, dic in enumerate(bars):
                try:
                    b = BarData()
                    b.symbol = dic["symbol"]

                    b.exchange = Exchange.BITMEX.value
                    b.open_price = float(dic["open"])
                    b.high_price = float(dic["high"])
                    b.low_price = float(dic["low"])
                    b.close_price = float(dic["close"])

                    b.datetime = self.get_datetime_from_bitmex_time(dic["timestamp"])
                    b.date = b.datetime.strftime("%Y-%m-%d")
                    b.time = b.datetime.strftime("%H:%M:%S")

                    b.volume = float(dic["volume"])
                    b.interval = interval

                    b.gateway_name = "DB"

                    ret_bars.append(b)

                except Exception as ex:
                    self.write_error(
                        'error when convert bar:{},ex:{},t:{}'.format(dic, str(ex), traceback.format_exc()))
            return ret_bars

        except Exception as ex:
            log_service_manager.write_log(
                'exception in get:{},{},{}'.format(bitmex_symbol, str(ex), traceback.format_exc()))
            return ret_bars

    def write_error(self, msg):
        log_service_manager.write_log(msg)
