# coding=utf-8

"""
ticker file parse的作用是合并 trades数据与tick数据到同一个文件
按照每小时分割的原则，合并到每天每小时这样
"""
import json
import time
from collections import defaultdict

from tumbler.function.order_math import get_round_order_price
from tumbler.function.os_class import LineIterator, LineOutput
from tumbler.object import Direction


class TickerFileParse(object):
    """
    函数的处理出来格式

    买一价,买一量,买二价...,卖一价,卖一量...,卖十量,成交量,成交额,最新价(计价单位: currency),服务器时间戳(13位，毫秒),交易所时间戳(13位，毫秒)

    + 买在某个价位的量，  + 卖在某个价位的量

    """

    def __init__(self):
        self.depth_dir = "/Users/szh/Documents/data/bitmex/depth/btc_usd_swap"
        self.trades_dir = "/Users/szh/Documents/data/bitmex/trades/btc_usd_swap"

        self.target_dir = "/Users/szh/Documents/data/bitmex/merge/btc_usd_swap"

    def parse_trade_line(self, line):
        # https://www.bitmex.com/api/v1/trade?symbol=.BXBT&count=200&columns=price&reverse=true
        # bitmex 的 指数数据下载地址
        ret = []
        arr = line.split('|')
        info = arr[-1]

        js_arr = json.loads(info)
        for info in js_arr:
            timestamp = info["date"]
            price = info["price"]
            volume = float(price) * info["volume"]
            direction = info["type"]
            if direction == "buy":
                direction = Direction.LONG.value
            else:
                direction = Direction.SHORT.value

            ret.append((float(timestamp)/1000.0, direction, price, volume))
        return ret

    def run(self):
        def func1(s):
            s = s.split('/')[-1]
            filename = s.split('.')[0]
            t1, t2 = filename.split('-')
            return float(t1), float(t2)

        def func2(s):
            s = s.split('/')[-1]
            t = time.strptime(s, "%Y%m%d_%H%M%S.csv")
            timestamp = float(time.mktime(t))
            return timestamp

        depth_iterator = LineIterator(self.depth_dir, suffix=".csv", filename_parse_func=func2)
        trade_iterator = LineIterator(self.trades_dir, suffix=".txt", filename_parse_func=func1)

        output_iterator = LineOutput(self.target_dir)

        # 最好手动指定下第一根K 开始的时间
        pre_time = None
        while True:
            depth_line = depth_iterator.get_last_line()

            if not depth_line:
                break
            depth_iterator.pop()

            arr = depth_line.split(',')
            now_time = float(arr[-1]) / 1000.0

            if not trade_iterator.check_timestamp_bigger_than_inside(now_time):
                t1, t2 = trade_iterator.get_first_file_timestamp()
                if now_time > t2:
                    trade_iterator.locate_file_from_timestamp(now_time)
                continue

            if pre_time:
                buy_dict = defaultdict(float)
                sell_dict = defaultdict(float)
                while True:
                    trade_line = trade_iterator.get_last_line()
                    if trade_line:
                        new_line_arr = self.parse_trade_line(trade_line)

                        need_pop = False
                        timestamp = None
                        for timestamp, direction, price, volume in new_line_arr:
                            if pre_time <= timestamp <= now_time:
                                if direction == Direction.LONG.value:
                                    buy_dict[price] += volume
                                else:
                                    sell_dict[price] += volume
                                #print("do run")
                                need_pop = True

                            if timestamp < pre_time:
                                need_pop = True

                            if timestamp > now_time:
                                need_pop = False

                            # print(pre_time, timestamp, now_time, direction, price, volume, pre_time <= timestamp,
                            #       timestamp <= now_time, buy_dict, sell_dict)

                            # print(timestamp, pre_time, timestamp > pre_time, timestamp >= now_time)

                        if need_pop:
                            trade_iterator.pop()
                        else:
                            # print(timestamp, pre_time, timestamp > pre_time, timestamp >= now_time)
                            break
                    else:
                        break

                buy_items = list(buy_dict.items())
                buy_items.sort()

                buy_items = [str(x) + ":" + str(y) for (x, y) in buy_items]

                sell_items = list(sell_dict.items())
                sell_items.sort()

                sell_items = [str(x) + ":" + str(y) for (x, y) in sell_items]

                #print(buy_items, sell_items)
                arr.append('|'.join(buy_items))
                arr.append('|'.join(sell_items))

                output_iterator.write(','.join(arr), now_time)

            pre_time = now_time


def run():
    ticker = TickerFileParse()
    ticker.run()


def test1():
    # f = open("test.txt", "r")
    # for i in range(10):
    #     data = f.readline().strip()
    #     print(data, len(data), not data)
    # f.close()

    # import os
    # for root, dirs, files in os.walk("/Users/szh/Documents/data/bitmex/depth/btc_usd_swap", topdown=False):
    #     for name in files:
    #         print(os.path.join(root, name))
    #     for name in dirs:
    #         print(os.path.join(root, name))

    s = '[{"tid": "3c5616e9-6b5c-4e30-1f3b-7a33120d4ad8", "volume": 0.0, "price": 30405.0, "date": 1611273900100, "type": "buy"}]'
    js = json.loads(s)
    timestamp = js["date"]
    price = js["price"]
    tid = js["tid"]
    Type = js["type"]
    Volume = price * js["volume"]


def test2():
    line_iterator = LineIterator(u_dir="/Users/szh/Documents/data/bitmex/trades/btc_usd_swap", suffix=".txt")
    for i in range(16):
        line = line_iterator.get_last_line()
        print(line)
        line_iterator.pop()
        if not line:
            break


if __name__ == "__main__":
    run()
    # test1()
    # test2()
