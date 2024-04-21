# coding=utf-8

import time
import requests

from tumbler.gateway.huobi.base import REST_MARKET_HOST
from tumbler.constant import Exchange, Interval
from tumbler.function import urlencode, get_no_under_lower_symbol

from tumbler.data.data import DataClient
from tumbler.service.log_service import log_service_manager

PERIOD_MAPPING = {Interval.MINUTE.value: '1min', Interval.HOUR.value: '60min',
                  Interval.HOUR4.value: "4hour", Interval.DAY.value: '1day'}

PERIOD_MAPPING_ADD_ID = {
    "1min": 60,
    "60min": 3600,
    "4hour": 4 * 3600,
    "1day": 24 * 3600
}


class HuobiClient(DataClient):
    def make_request(self, symbol, interval, start_dt=None, end_dt=None, limit=100, use_proxy=False):
        try:
            url = REST_MARKET_HOST + "/market/history/kline"
            params = {"symbol": get_no_under_lower_symbol(symbol),
                      "period": interval, "limit": limit}
            log_service_manager.write_log("[make_request] url:{}/{}".format(url, urlencode(params)))
            response = requests.request("GET", url, params=params, timeout=15)
            data = response.json()
            ret = []
            for dic in data["data"]:
                ret.append([dic["id"] * 1000, dic["open"], dic["high"], dic["low"], dic["close"],
                            dic["vol"], (dic["id"] + PERIOD_MAPPING_ADD_ID[interval]) * 1000,
                            dic["amount"], dic["count"], 0, 0, 0])
            ret.sort()
            return ret
        except Exception as ex:
            msg = f"[make_request] ex symbol:{symbol}, interval:{interval}, {start_dt}, {end_dt}, ex:{ex}"
            log_service_manager.write_log(msg)
            time.sleep(3)
            return self.make_request(symbol, interval, start_dt, end_dt, limit)

    def get_format_symbol(self, symbol):
        return symbol.lower().replace('_', '').replace('/', '')

    def get_format_period(self, period):
        return PERIOD_MAPPING.get(period)

    def get_exchange(self):
        return Exchange.HUOBI.value
