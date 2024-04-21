# coding=utf-8

from datetime import datetime
import time
import traceback
import requests
import json

from tumbler.service.mysql_service import MysqlService
from tumbler.function import parse_timestamp_get_str

class CoinMarket(object):

    def __init__(self):
        pass

    def get_latested_info(self, limit=100, start=1, try_times=0):
        latest_url = "https://web-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?convert=USD&cryptocurrency_type=all&limit={}&sort=market_cap&sort_dir=desc&start={}".format(
            limit, start)
        print(latest_url)
        try:
            data = requests.get(latest_url)
        except Exception as ex:
            if try_times < 3:
                return self.get_latested_info(limit, start, try_times + 1)
            else:
                return {}
        return data.json()

    def run_update(self):
        data = self.get_latested_info(limit=5000, start=1)
        mysql_service_manager = MysqlService.get_mysql_service()
        for dic in data["data"]:
            asset_code = dic.get("symbol", "")
            asset_name = dic.get("slug", "")
            tags_arr = dic.get("tags", [])
            create_date = parse_timestamp_get_str(dic.get("date_added", ""))
            max_supply = dic.get("max_supply", 0)
            if not max_supply:
                max_supply = dic.get("total_supply", 0)

            try:
                mysql_service_manager.update_asset_info(asset_code, name=asset_name, tags=tags_arr,
                                                        create_date=create_date, max_supply=max_supply)
            except Exception as ex:
                print(ex, dic, asset_code, asset_name)


if __name__ == "__main__":
    coinMarket = CoinMarket()
    # data = coinMarket.get_latested_info(limit=100, start=1)
    # print(data)

    coinMarket.run_update()
