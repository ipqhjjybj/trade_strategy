# coding=utf-8

import zlib
import websockets
import json
import asyncio

from datetime import datetime

from tumbler.constant import Exchange, Interval

from tumbler.constant import Interval
from tumbler.data.huobi_data import HuobiClient

from tumbler.constant import Exchange, Interval
import tumbler.config as config


# import socket
# import socks
# socks.set_default_proxy(socks.HTTP, addr=config.SETTINGS["proxy_host"],
#                                         port=config.SETTINGS["proxy_port"])
# socket.socket = socks.socksocket


def download_huobi_data():
    b = HuobiClient()
    # b.get_kline("BCH/USDT", Interval.MINUTE.value, start_dt=datetime(2020, 5, 1), end_dt=datetime(2020, 5, 2))
    # data = b.get_kline("BTC/USDT", Interval.HOUR.value, start_dt=datetime(2020, 1, 1), end_dt=datetime(2020, 5, 2))
    # data = b.make_request("btcusdt", "60min", start_dt=1590187891, end_dt=1590997891, limit=1000)
    # print(data)
    for i in range(3):
        data = b.make_request("btcusdt", "60min", start_dt=1590187891, end_dt=1590187891, limit=1000, use_proxy=True)
        print(data)


if __name__ == "__main__":
    download_huobi_data()
    # get_huobi_data("btcusdt", "4hour", 1501174800, 2556115200)
    # symbol = "btcusdt"
    # period = "4hour"
    # t_from = 1501174800
    # t_to = 2556115200
    # msg = '{req: "market.{}.kline.{}","id": "id1","from": {},"to": {}}'.format(symbol, period, t_from, t_to)
    # print(msg)
    # msg = """{
    #             "req": "market.%s.kline.%s",
    #             "id": "id1",
    #             "from": %s,
    #             "to": %s
    #         }""" % (symbol, period, str(t_from), str(t_to))
    # print(msg)

    # asyncio.get_event_loop().run_until_complete(coroutine)
