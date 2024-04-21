# coding=utf-8

from datetime import datetime
import time
import traceback
import requests
from pprint import pprint

from tumbler.object import BarData
from tumbler.constant import Exchange, Interval
from tumbler.gateway.okex5.base import REST_MARKET_HOST, okex5_format_symbol
from tumbler.function import urlencode, datetime_2_time
from tumbler.function import get_format_lower_symbol, get_vt_key
from tumbler.service.log_service import log_service_manager
from tumbler.data.data import DataClient
from tumbler.constant import Interval

PERIOD_MAPPING = {
    Interval.MINUTE.value: '1m',
    Interval.MINUTE3.value: '3m',
    Interval.MINUTE5.value: '5m',
    Interval.MINUTE15.value: '15m',
    Interval.MINUTE30.value: '30m',
    Interval.HOUR.value: '1H',
    Interval.HOUR2.value: '2H',
    Interval.HOUR4.value: '4H',
    Interval.HOUR6.value: '6H',
    Interval.HOUR12.value: '12H',
    Interval.DAY.value: '1D',
    Interval.WEEK.value: '1W',
    Interval.MONTH.value: '1M',
    Interval.MONTH3.value: '3M',
    Interval.MONTH6.value: '6M',
    Interval.YEAR.value: '1Y'
}


class Okex5Client(DataClient):
    def make_request(self, symbol, interval, start_dt=None, end_dt=None, limit=1440):
        try:
            params = {
                "instId": okex5_format_symbol(symbol),
                "bar": interval,
                "limit": limit
            }
            if start_dt:
                if isinstance(start_dt, datetime):
                    ins_time = int(datetime_2_time(start_dt) * 1000) - 1
                else:
                    ins_time = start_dt - 1
                params["before"] = ins_time
            elif end_dt:
                if isinstance(start_dt, datetime):
                    ins_time = int(datetime_2_time(end_dt) * 1000) + 1
                else:
                    ins_time = end_dt + 1
                params["after"] = ins_time
            else:
                msg = "[Okex5Client] [make_request] error, datetime empty error!"
                log_service_manager.write_log(msg)
                return []

            url = REST_MARKET_HOST + "/api/v5/market/candles"
            response = requests.request("GET", url, params=params, timeout=15)
            log_service_manager.write_log("[make_request] url:{}?{}".format(url, urlencode(params)))
            data = response.json()
            ret = []
            for arr in data["data"]:
                # ts,open,high,low,close,vol,volCcy
                arr = [float(x) for x in arr]
                arr[0] = int(arr[0])
                arr = arr[:-1] + [0] * 6

                ret.append(arr)
            ret.sort(reverse=True)
            return ret
        except Exception as ex:
            msg = f"[make_request] ex symbol:{symbol}, interval:{interval}, {start_dt}, {end_dt}, ex:{ex}"
            log_service_manager.write_log(msg)
            time.sleep(3)
            return self.make_request(symbol, interval, start_dt, end_dt, limit)

    @staticmethod
    def get_available_interval():
        return list(PERIOD_MAPPING.keys())

    def get_format_period(self, period):
        return PERIOD_MAPPING[period]

    def get_exchange(self):
        return Exchange.OKEX5.value

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
                                                          start_dt=None, end_dt=end_timestamp, limit=100)
                        if len(bars_per_loop) == 0:
                            log_service_manager.write_log(u"bars' len is zero")
                            break

                        # 更新开始时间，为这次取得最后一个值
                        end_timestamp = bars_per_loop[-1][0] + 1
                        if len(bars) > 0:
                            while bars[-1][0] <= bars_per_loop[0][0]:
                                if len(bars_per_loop) == 1:
                                    bars_per_loop = []
                                    end_timestamp = start_timestamp + 1
                                    break
                                bars_per_loop.pop(0)

                        # 追加bars
                        bars.extend(bars_per_loop)

                    except Exception as ex:
                        self.write_error(
                            u'Download data {} {} has error:{},{}'.format(symbol, period, str(ex),
                                                                          traceback.format_exc()))
                        break

            bars.reverse()
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


if __name__ == "__main__":
    from tumbler.object import BarData

    symbol = "btm_usdt"
    interval = Interval.MINUTE.value
    okex_data_client = Okex5Client()

    # bars_data = okex_data_client.make_request(symbol,
    #                                           interval,
    #                                           start_dt=datetime(2021, 9, 7, 16, 20),
    #                                           end_dt=None, limit=100)
    #
    # print(bars_data)

    # datetime(2021, 9, 1, 0, 0)
    bars = okex_data_client.get_kline(symbol, interval,
                                      start_datetime=datetime(2021, 9, 1, 0, 0),
                                      end_datetime=datetime.now())
    df = BarData.get_pandas_from_bars(bars)
    df.to_csv("test.csv")

    from datetime import timedelta

    flag = True
    arr = list(df["datetime"])
    for i in range(len(arr) - 1):
        if arr[i] + timedelta(minutes=1) == arr[i + 1]:
            pass
        else:
            print("not ok", arr[i], arr[i + 1])
            flag = False

    if flag:
        print("all right!")
    else:
        print("not right!")
