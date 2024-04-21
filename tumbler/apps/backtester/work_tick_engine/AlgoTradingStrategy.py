# coding=utf-8
import sys
from copy import copy
import numpy as np
# from qpython import *
import heapq
import time
from scipy.optimize import curve_fit
import math
from collections import defaultdict
from datetime import datetime, timedelta

from tumbler.apps.backtester.work_tick_engine.new_tick_engine import *
from tumbler.function.os_class import LineIterator
from tumbler.function import datetime_2_time, time_2_datetime

'''
下单算法交易优化升级模块

1. 接到信号大概提前5分钟左右, 然后开始买入指定仓位
2. 目标奖励R是: 买入的均价与该小时信号的收盘价的差值

参数: 
    period: 下单的周期， 比如1，表示每小时下单，4小时4小时下单一次
    pre_minutes: 提前交易的分钟数
    vol: 每次需要完成的下单量
    direction: 方向， 默认long

'''


class AlgoTradingStrategy(Strategy):
    def __init__(self, recordFloatPnL=False):
        super(AlgoTradingStrategy, self).__init__(recordFloatPnL)

        self.symbol = "btc_usdt"
        # arguments
        self.period = 1
        self.pre_minutes = 5
        self.vol = 10
        self.direction = 'B'

        # variable
        self.need_work_vol = 0
        self.now_work_vol = 0

        self.delay_time = 10  # 至少等待10秒发一次单

        # strategy record
        self.compare_cost_sum = 0
        self.real_cost_sum = 0

        # system variable
        self.datetime = None
        self.now_time = 0

        self.order_dict = {}

        self.working_times = 0
        self.pre_working_flag = False
        self.pre_send_order_time = 0
        self.pre_cost_sum_price = 0
        self.save_ticker = None

    def update_datetime(self, tick):
        self.datetime = tick["date"]
        self.now_time = datetime_2_time(self.datetime)

    def OnInit(self, engine):
        super(AlgoTradingStrategy, self).OnInit(engine)

    def UpdateNav(self):
        super(AlgoTradingStrategy, self).UpdateNav()

    def sendOrder(self, symbol, BorS, OorC, price, qty):
        return super(AlgoTradingStrategy, self).sendOrder(symbol, BorS, OorC, price, qty)

    def cancel_all_order(self):
        order_list = list(self.order_dict.keys())
        for oid in order_list:
            self.cancelOrder(oid)
            self.order_dict.pop(oid)

    def cancelOrder(self, oid):
        return super(AlgoTradingStrategy, self).cancelOrder(oid)

    def working(self):
        pass

    def OnTick(self, tick):
        self.save_ticker = copy(tick)
        self.update_datetime(tick)

        delay_time = self.datetime + timedelta(minutes=self.pre_minutes)
        if delay_time.hour % self.period == 0 and delay_time.minute <= self.pre_minutes:
            # print("[working] datetime:{} delay_time:{}".format(self.datetime, delay_time))
            self.pre_working_flag = True

            if self.now_time > self.pre_send_order_time + self.delay_time:
                self.cancel_all_order()
                target_pos = self.vol * (self.working_times + 1)
                if self.pos < target_pos:
                    o_order = self.sendOrder(self.symbol, BorS='B', OorC='O',
                                             price=tick["bid1"], qty=target_pos-self.pos)
                    self.order_dict[o_order.id] = copy(o_order)

                    self.pre_send_order_time = self.now_time

        else:
            if self.pre_working_flag:
                self.working_times += 1

                self.compare_cost_sum += tick["ask1"] * self.vol
                self.pre_cost_sum_price = tick["ask1"]

            self.pre_working_flag = False

            if self.vol * self.working_times > self.pos:
                volume = self.vol * self.working_times - self.pos
                self.cancel_all_order()

                o_order = self.sendOrder(self.symbol, BorS='B', OorC='O', price=tick["ask1"], qty=volume)
                self.order_dict[o_order.id] = copy(o_order)

    def OnTrade(self, trd):
        super(AlgoTradingStrategy, self).OnTrade(trd)

        if trd.side == self.direction:
            self.real_cost_sum += trd.price * trd.qty

        self.write_log("{},{} pos:{} nav:{} compare_cost_sum:{} real_cost_sum:{} bid1:{} ask1:{} pre_cost_price:{}"
                       .format(trd, trd.msg, self.pos, self.compare_cost_sum - self.real_cost_sum,
                               self.compare_cost_sum, self.real_cost_sum, self.save_ticker["bid1"],
                               self.save_ticker["ask1"], self.pre_cost_sum_price))
        if trd.id in self.order_dict.keys():
            self.order_dict.pop(trd.id)

    def OnFinish(self):
        super(AlgoTradingStrategy, self).OnFinish()

    def FindOrder(self, id):
        super(AlgoTradingStrategy, self).FindOrder(id)

    def write_log(self, msg):
        print("{},{}".format(self.datetime, msg))


def run():
    def func1(s):
        s = s.split('/')[-1]
        t = time.strptime(s, "%Y%m%d_%H%M%S.csv")
        timestamp = float(time.mktime(t))
        return timestamp

    # trades_dir = "/Users/szh/Documents/data/bitmex/merge/btc_usd_swap/20210123"
    trades_dir = "/Users/szh/Documents/data/bitmex/merge/btc_usd_swap"

    # trades_dir = "/Users/szh/Documents/data/bitmex/merge/btc_usd_swap/test"

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
            tickers[i]['bid{}'.format(j)] = float(arr[2 * j - 2])
            # 2, 4, 6 --> 每个-1
            tickers[i]['bidvol{}'.format(j)] = float(arr[2 * j - 1])
            # 21, 23, 25  --> 每个-1
            tickers[i]['ask{}'.format(j)] = float(arr[2 * j + 18])
            # 22, 24, 26  --> 每个-1
            tickers[i]['askvol{}'.format(j)] = float(arr[2 * j + 19])

        tickers[i]['volume'] = float(arr[40])
        tickers[i]['amount'] = float(arr[41])
        tickers[i]['price'] = float(arr[42])
        tickers[i]['server_time'] = int(arr[43])
        tickers[i]['exchange_time'] = int(arr[44])

        if i == 0:
            tickers[i]['bfPrice'] = tickers[i]['bid1']
            tickers[i]['afPrice'] = tickers[i]['ask1']
        else:
            tickers[i]['bfPrice'] = tickers[i - 1]['bid1']
            tickers[i]['afPrice'] = tickers[i - 1]['ask1']

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
        tickers[i]['mid'] = (tickers[i]['bid1'] + tickers[i]['ask1']) / 2.0

    tickers = pd.DataFrame(tickers)
    strategy = AlgoTradingStrategy()
    engine = TickEngine()
    engine.RegisterStrategy(strategy)
    engine.Start(tickers)
    return strategy


run()
