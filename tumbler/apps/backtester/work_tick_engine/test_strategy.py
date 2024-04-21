# coding=utf-8
import sys
import numpy as np
# from qpython import *
import heapq
import time
from datetime import datetime

from tumbler.apps.backtester.work_tick_engine.new_tick_engine import *
from tumbler.service import log_service_manager
from tumbler.function.os_class import LineIterator


class TestStrategy(Strategy):
    def __init__(self, recordFloatPnL=False):
        super(TestStrategy, self).__init__(recordFloatPnL)

    def OnInit(self, engine):
        super(TestStrategy, self).OnInit(engine)

    def UpdateNav(self):
        super(TestStrategy, self).UpdateNav()

    def sendOrder(self, symbol, BorS, OorC, price, qty):
        return super(TestStrategy, self).sendOrder(symbol, BorS, OorC, price, qty)

    def cancelOrder(self, oid):
        return super(TestStrategy, self).cancelOrder(oid)

    def OnTick(self, tick):
        #log_service_manager.write_log("[onTick] t:{} {}".format(tick["bid1"], tick["ask1"]))
        if self.pos == 0:
            order = self.sendOrder("btc_usdt", 'B', 'O', tick["ask1"], 1)
            log_service_manager.write_log("[onTick] order:{}".format(order.__dict__))
        elif self.pos > 0:
            order = self.sendOrder("btc_usdt", 'S', 'O', tick["bid1"], 1)
            log_service_manager.write_log("[onTick] order:{}".format(order.__dict__))
        elif self.pos < 0:
            order = self.sendOrder("btc_usdt", 'B', 'O', tick['ask1'], 1)
            log_service_manager.write_log("[onTick] order:{}".format(order.__dict__))

    def OnTrade(self, trd):
        super(TestStrategy, self).OnTrade(trd)

    def OnFinish(self):
        super(TestStrategy, self).OnFinish()

    def FindOrder(self, id):
        super(TestStrategy, self).FindOrder(id)


def run():
    def func1(s):
        s = s.split('/')[-1]
        t = time.strptime(s, "%Y%m%d_%H%M%S.csv")
        timestamp = float(time.mktime(t))
        return timestamp
    #trades_dir = "/Users/szh/Documents/data/bitmex/merge/btc_usd_swap/20210123"
    trades_dir = "/Users/szh/Documents/data/bitmex/merge/btc_usd_swap/test"

    line_iterator = LineIterator(trades_dir, suffix=".csv", filename_parse_func=func1)
    all_lines = []
    while True:
        depth_line = line_iterator.get_last_line()

        if not depth_line:
            break
        line_iterator.pop()
        all_lines.append(depth_line)

    symbol = "btc_usdt"
    tickers = np.recarray((len(all_lines),), dtype=[
        ('symbol', np.str_, 16),
        ('bid1', float),
        ('bidvol1', float),
        ('bid2', float),
        ('bidvol2', float),
        ('bid3', float),
        ('bidvol3', float),
        ('bid4', float),
        ('bidvol4', float),
        ('bid5', float),
        ('bidvol5', float),
        ('bid6', float),
        ('bidvol6', float),
        ('bid7', float),
        ('bidvol7', float),
        ('bid8', float),
        ('bidvol8', float),
        ('bid9', float),
        ('bidvol9', float),
        ('bid10', float),
        ('bidvol10', float),
        ('ask1', float),
        ('askvol1', float),
        ('ask2', float),
        ('askvol2', float),
        ('ask3', float),
        ('askvol3', float),
        ('ask4', float),
        ('askvol4', float),
        ('ask5', float),
        ('askvol5', float),
        ('ask6', float),
        ('askvol6', float),
        ('ask7', float),
        ('askvol7', float),
        ('ask8', float),
        ('askvol8', float),
        ('ask9', float),
        ('askvol9', float),
        ('ask10', float),
        ('askvol10', float),
        ('volume', float),
        ('amount', float),
        ('price', float),
        ('server_time', int),
        ('exchange_time', int),
        ('max_buy_price', float),
        ('max_buy_price_volume', float),
        ('min_sell_price', float),
        ('min_sell_price_volume', float),
        ('askFill', float),
        ('bidFill', float),
        ('afPrice', float),
        ('bfPrice', float),
        ('mid', float),
        ('date', object),
    ])

    for i in range(len(tickers)):
        line = all_lines[i]
        arr = line.strip().split(',')

        tickers[i]['symbol'] = symbol
        for j in range(1, 11):
            # 1, 3, 5 --> 每个-1
            tickers[i]['bid{}'.format(j)] = float(arr[2*j-2])
            # 2, 4, 6 --> 每个-1
            tickers[i]['bidvol{}'.format(j)] = float(arr[2*j-1])
            # 21, 23, 25  --> 每个-1
            tickers[i]['ask{}'.format(j)] = float(arr[2*j+18])
            # 22, 24, 26  --> 每个-1
            tickers[i]['askvol{}'.format(j)] = float(arr[2*j+19])

        tickers[i]['volume'] = float(arr[40])
        tickers[i]['amount'] = float(arr[41])
        tickers[i]['price'] = float(arr[42])
        tickers[i]['server_time'] = int(arr[43])
        tickers[i]['exchange_time'] = int(arr[44])

        if i == 0:
            tickers[i]['bfPrice'] = tickers[i]['bid1']
            tickers[i]['afPrice'] = tickers[i]['ask1']
        else:
            tickers[i]['bfPrice'] = tickers[i-1]['bid1']
            tickers[i]['afPrice'] = tickers[i-1]['ask1']

        askfill = 0
        bidfill = 0
        buy_map_str = arr[45]
        sell_map_str = arr[46]

        max_buy_price = 0
        max_buy_price_volume = 0
        if buy_map_str:
            buy_arr = buy_map_str.strip().split('|')
            for s_item in buy_arr:
                p, v = s_item.strip().split(':')
                max_buy_price = max(max_buy_price, float(p))

            for s_item in buy_arr:
                p, v = s_item.strip().split(':')
                if abs(max_buy_price - float(p)) < 1e-12:
                    max_buy_price_volume += float(v)

                if tickers[i]['bfPrice'] + 1e-12 >= float(p):
                    bidfill += float(v)

                if tickers[i]['afPrice'] <= float(p) + 1e-12:
                    askfill += float(v)

        min_sell_price = 1e9
        min_sell_price_volume = 0
        if sell_map_str:
            sell_arr = sell_map_str.strip().split('|')
            for s_item in sell_arr:
                p, v = s_item.strip().split(':')
                min_sell_price = min(min_sell_price, float(p))

            for s_item in sell_arr:
                p, v = s_item.strip().split(':')
                if abs(min_sell_price - float(p)) < 1e-12:
                    min_sell_price_volume += float(v)

                if tickers[i]['bfPrice'] + 1e-12 >= float(p):
                    bidfill += float(v)

                if tickers[i]['afPrice'] <= float(p) + 1e-12:
                    askfill += float(v)

        tickers[i]['max_buy_price'] = max_buy_price
        tickers[i]['max_buy_price_volume'] = max_buy_price_volume
        tickers[i]['min_sell_price'] = min_sell_price
        tickers[i]['min_sell_price_volume'] = min_sell_price_volume

        tickers[i]['askFill'] = askfill
        tickers[i]['bidFill'] = bidfill

        tickers[i]['date'] = datetime.fromtimestamp(tickers[i]['exchange_time'] / 1000.0)
        tickers[i]['mid'] = (tickers[i]['bid1'] + tickers[i]['ask1'])/2.0

    tickers = pd.DataFrame(tickers)
    strategy = TestStrategy()
    engine = TickEngine()
    engine.RegisterStrategy(strategy)
    engine.Start(tickers)
    return strategy


run()
