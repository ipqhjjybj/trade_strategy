# coding=utf-8

import time
import requests
from datetime import datetime

from tumbler.constant import Exchange, Interval
from tumbler.service.mysql_service import MysqlService
from tumbler.gateway.binance.base import REST_MARKET_HOST
from tumbler.gateway.binance.base import change_system_format_to_binance_format
from tumbler.function import urlencode
from tumbler.service.log_service import log_service_manager
from tumbler.data.data import DataClient

PERIOD_MAPPING = {
    Interval.MINUTE.value: '1m',
    Interval.MINUTE3.value: '3m',
    Interval.MINUTE5.value: '5m',
    Interval.MINUTE15.value: '15m',
    Interval.HOUR.value: '1h',
    Interval.HOUR2.value: '2h',
    Interval.HOUR4.value: '4h',
    Interval.HOUR6.value: '6h',
    Interval.HOUR12.value: '12h',
    Interval.DAY.value: '1d',
    Interval.WEEK.value: '1w',
    Interval.MONTH.value: '1M'
}


# PERIOD_MAPPING['3min']   = '3m'
# PERIOD_MAPPING['5min']   = '5m'
# PERIOD_MAPPING['15min']  = '15m'
# PERIOD_MAPPING['30min']  = '30m'
# PERIOD_MAPPING['1hour']  = '1h'
# PERIOD_MAPPING['2hour']  = '2h'
# PERIOD_MAPPING['4hour']  = '4h'
# PERIOD_MAPPING['6hour']  = '6h'
# PERIOD_MAPPING['8hour']  = '8h'
# PERIOD_MAPPING['12hour'] = '12h'
# PERIOD_MAPPING['1day']   = '1d'
# PERIOD_MAPPING['3day']   = '3d'
# PERIOD_MAPPING['1week']  = '1w'
# PERIOD_MAPPING['1month'] = '1M'


class BinanceClient(DataClient):
    def make_request(self, symbol, interval, start_dt=None, end_dt=None, limit=1000):
        try:
            url = REST_MARKET_HOST + "/api/v3/klines"
            params = {"symbol": symbol, "interval": interval, "startTime": start_dt, "endTime": end_dt, "limit": limit}
            log_service_manager.write_log("[make_request] url:{}/{}".format(url, urlencode(params)))
            response = requests.request("GET", url, params=params, timeout=15)
            data = response.json()
            return data
        except Exception as ex:
            msg = f"[make_request] ex symbol:{symbol}, interval:{interval}, {start_dt}, {end_dt}, ex:{ex}"
            log_service_manager.write_log(msg)
            time.sleep(3)
            return self.make_request(symbol, interval, start_dt, end_dt, limit)

    @staticmethod
    def get_available_interval():
        return list(PERIOD_MAPPING.keys())

    def get_format_symbol(self, symbol):
        symbol = symbol.upper().replace('_', '')
        return change_system_format_to_binance_format(symbol)

    def get_format_period(self, period):
        return PERIOD_MAPPING.get(period)

    def get_exchange(self):
        return Exchange.BINANCE.value

    def get_asset_info(self):
        '''
        {
           "code":"000000",
           "message":null,
           "messageDetail":null,
           "data":[
              {
                 "id":"512",
                 "assetCode":"INJ",
                 "assetName":"Injective Protocol",
                 "unit":"",
                 "commissionRate":0.000000,
                 "freeAuditWithdrawAmt":0.00000000,
                 "freeUserChargeAmount":1000000.00000000,
                 "createTime":1603080237000,
                 "test":0,
                 "gas":null,
                 "isLegalMoney":false,
                 "reconciliationAmount":0.00000000,
                 "seqNum":"0",
                 "chineseName":"Injective Protocol",
                 "cnLink":"",
                 "enLink":"",
                 "logoUrl":"https://bin.bnbstatic.com/image/admin_mgs_image_upload/20201019/383e18cf-53cd-44b9-b781-4e85818ecfb8.png",
                 "fullLogoUrl":"https://bin.bnbstatic.com/image/admin_mgs_image_upload/20201019/383e18cf-53cd-44b9-b781-4e85818ecfb8.png",
                 "supportMarket":null,
                 "feeReferenceAsset":null,
                 "feeRate":null,
                 "feeDigit":8,
                 "assetDigit":8,
                 "trading":true,
                 "tags":[
                    "defi",
                    "pos",
                    "BSC"
                 ],
                 "plateType":"MAINWEB",
                 "etf":false
              },
            ]
        }
        '''
        url = "https://www.binance.com/bapi/asset/v2/public/asset/asset/get-all-asset"
        try:
            response = requests.get(url, params={}, timeout=15)
            data = response.json()
            mysql_service_manager = MysqlService.get_mysql_service()
            for dic in data["data"]:
                asset_code = dic.get("assetCode", "")
                asset_name = dic.get("assetName", "")
                tags_arr = dic.get("tags", [])

                mysql_service_manager.update_asset_info(asset_code, name=asset_name, tags=tags_arr)

        except Exception as ex:
            msg = f"[get_asset_info] ex:{ex}"
            log_service_manager.write_log(msg)


if __name__ == "__main__":
    from tumbler.object import BarData

    symbol = "btc_usdt"
    interval = Interval.MINUTE.value
    binance_data_client = BinanceClient()

    # bars_data = binance_data_client.make_request(symbol, interval, start_dt=1630997040000,
    #                                           end_dt=1631002920000, limit=100)
    #
    # print(bars_data)
    bars = binance_data_client.get_kline(symbol,
                                         interval,
                                         start_datetime=datetime(2021, 9, 7, 16, 20),
                                         end_datetime=None)
    df = BarData.get_pandas_from_bars(bars)
    print(df)
