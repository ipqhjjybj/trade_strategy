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


class AsqMakerStrategy(Strategy):
    """
    1. 公式是错误的
    2. 报价改变的话，原先订单需要先撤销。而不是不撤
    3. 强制至少3秒后才能撤单      (一个可变调参)
    4. 时间窗口 采样频率 30笔，60笔, 90笔  (是一个可变调参)

    𝜆(𝛿) = A exp(−𝑘𝛿)
    𝛿可以理解为市价买单或者卖单所越过的最深价 格与中间价的距离

    波动率𝜎可 以直接从利用高频数据的中间价计算
    限价指令簿厚度系数𝜅和限价指令簿击穿概率系数𝐴则 需要通过统计市价单击穿某个价位的概率
    在统计时，把市价买单和市价卖单分开进行，分 别计算出当天的𝜅值和 A 值。然后对其取算术平均得到最终的𝜅值和 A 值，这么做是为了对买 卖双向不做偏好。
    """

    def __init__(self, recordFloatPnL=False):
        super(AsqMakerStrategy, self).__init__(recordFloatPnL)

        self.cache_data_num = 100  # 缓存多少根数据 用于计算
        self.sample_duration = 10
        # self.sample_duration = 10        # 采样间隔是多少根tick

        self.delta_clear_time = timedelta(hours=4)

        self.u = 0.01

        self.cache_ticker_last_prices = np.array([0] * self.sample_duration)

        self.cache_bid_prices = np.array([0] * self.cache_data_num)
        self.cache_ask_prices = np.array([0] * self.cache_data_num)

        self.count = 1

        self.last_ticker = None
        self.last_sample_ticker = None
        self.bid_cross = 0
        self.max_bid_cross = 0
        self.ask_cross = 0
        self.max_ask_cross = 0

        self.q = 0  # 期货持仓
        self.r = 0  # 中间价
        self.o = 0  # 波动率

        self.S = 0  # 盘口差价

        self.A_ask = 0  # 击穿概率系数 ask
        self.A_bid = 0  # 击穿概率系数 bid
        self.K_ask = 0  # 盘口厚度系数 ask
        self.K_bid = 0  # 盘口厚度系数 bid

        self.A = 0  # 击穿概率系数
        self.K = 0  # 盘口厚度系数

        self.max_hold_time = 0  # 目标持有时间

        self.order_bid = 0  # 目标买单价格
        self.order_ask = 0  # 目标卖单价格

        self.order_dict = {}  # 订单管理队列
        self.min_wait_time = 3  # 订单最少挂单时间

        self.count_cache_num = 0  # 计算cache num数量

        self.datetime = None
        self.clear_time = None
        self.save_ticker = None

    def update_datetime(self, tick):
        self.datetime = tick["date"]

    def OnInit(self, engine):
        super(AsqMakerStrategy, self).OnInit(engine)

    def UpdateNav(self):
        super(AsqMakerStrategy, self).UpdateNav()

    def sendOrder(self, symbol, BorS, OorC, price, qty):
        return super(AsqMakerStrategy, self).sendOrder(symbol, BorS, OorC, price, qty)

    def cancelOrder(self, oid):
        return super(AsqMakerStrategy, self).cancelOrder(oid)

    def compute_a_r(self, cache_prices):
        def func(x, a, k):
            return a * np.exp(-k * x)

        val_map = defaultdict(float)
        for price in cache_prices:
            val_map[price] += 1.0 / self.cache_data_num

        xdata = []
        ydata = []
        for x, y in val_map.items():
            xdata.append(x)
            ydata.append(y)

        npv = sum(cache_prices) * 1.0 / len(cache_prices)  # 均值

        popt, pcov = curve_fit(func, xdata, ydata)
        # self.write_log(
        #     "[compute_a_r] {} {} {} {} {} {}".format(cache_prices, popt, xdata, ydata, npv, 1.0 / npv))
        return popt
        #return npv, 1.0 / npv

    def clear_position(self):
        pass
        #self.write_log("[clear_position] clear_position!")

    def cancel_all_orders(self):
        order_list = list(self.order_dict.keys())
        for oid in order_list:
            self.cancelOrder(oid)
            self.order_dict.pop(oid)

    def OnTick(self, tick):
        self.save_ticker = copy(tick)
        self.q = self.pos
        self.update_datetime(tick)

        now_t = datetime_2_time(tick["date"])

        if not self.clear_time or self.datetime > self.clear_time:
            if self.clear_time:
                self.clear_position()
            self.clear_time = self.datetime + self.delta_clear_time

        if self.last_ticker:
            self.bid_cross = self.last_ticker["bid1"] - tick["bid1"]
            self.ask_cross = tick["ask1"] - self.last_ticker["ask1"]

            self.max_bid_cross = max(self.bid_cross, self.max_bid_cross)
            self.max_ask_cross = max(self.ask_cross, self.max_ask_cross)

            #
            # avg_bid = self.cache_bid_prices.mean()
            # avg_ask = self.cache_ask_prices.mean()
            #
            # self.o = self.cache_bid_prices.std()
            #
            # self.k = (avg_bid + avg_ask) / 2.0
            #
            # self.r = tick["mid"] - self.q * self.u * self.o * self.o * (self.T - self.t)
            #
            # self.S = 2.0 / self.u * math.log(1 + self.u / self.k) + 0.5 * self.u * self.o * self.o * (
            #         self.T - self.t) * (self.T - self.t)
            #
            # self.order_bid = self.r - self.S / 2
            # self.order_ask = self.r + self.S / 2
            #
            # self.write_log("[info] {},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(
            #     self.bid_cross, self.ask_cross, self.max_bid_cross, self.max_ask_cross, avg_bid, avg_ask,
            #     self.o, tick["mid"], self.r, self.S, self.order_bid, self.order_ask, risk, self.q))
            #
            # if self.count > 200:
            #     if self.order_bid > 0:
            #         order = self.sendOrder("btc_usdt", 'B', 'O', min(self.order_bid, tick["bid1"]), 1)
            #         self.write_log("[onTick] bid_order:{}".format(order.__dict__))
            #
            #     if self.order_ask > 0:
            #         order = self.sendOrder("btc_usdt", 'S', 'O', max(self.order_bid, tick["ask1"]), 1)
            #         self.write_log("[onTick] ask_order:{}".format(order.__dict__))

        if self.count % self.sample_duration == 0:
            # self.write_log("[count] {}".format(self.count))
            if self.last_sample_ticker:
                last_ticker_time = datetime_2_time(self.last_sample_ticker["date"])

                end_time = datetime_2_time(self.clear_time)

                use_time = (end_time - now_t) / (now_t - last_ticker_time)

                self.count_cache_num += 1
                self.cache_bid_prices = np.append(self.cache_bid_prices[1:], self.max_bid_cross)
                self.cache_ask_prices = np.append(self.cache_ask_prices[1:], self.max_ask_cross)

                self.cache_ticker_last_prices = np.append(self.cache_ticker_last_prices[1:], tick["mid"])

                if self.count_cache_num > self.cache_data_num:
                    self.o = self.cache_ticker_last_prices.std()
                    self.A_ask, self.K_ask = self.compute_a_r(self.cache_ask_prices)
                    self.K_ask, self.K_bid = self.compute_a_r(self.cache_bid_prices)
                    self.A = (self.A_ask + self.K_ask) / 2.0
                    self.K = (self.K_ask + self.K_bid) / 2.0

                    self.r = tick["mid"] - self.q * self.u * self.o * self.o * use_time
                    self.S = 2.0 / self.u * math.log(
                        1 + self.u / self.K) + 1.0 / 2.0 * self.u * self.o * self.o * use_time * use_time

                    self.S = 2.0 / self.u * math.log(
                        1 + self.u / self.K) + 1.0 / 2.0 * self.u * self.o * self.o * use_time

                    tmp = 1.0 / 2.0 * self.u * self.o * self.o * use_time

                    self.order_bid = self.r - self.S / 2.0
                    self.order_ask = self.r + self.S / 2.0

                    new_order_bid = tick["mid"] - (1.0 / self.u * math.log(1 + self.u/self.K) + (1+2*self.q)/2 * self.u * self.o * self.o * use_time)
                    new_order_ask = tick["mid"] + (1.0 / self.u * math.log(1 + self.u/self.K) + (1-2*self.q)/2 * self.u * self.o * self.o * use_time)

                    #new_order_bid = 1.0 / self.u * math.log(1 + self.u/self.K) - (2*self.q-1)/2.0 * math.sqrt()

                    self.cancel_all_orders()

                    # self.write_log("[ans] [{}], {},{},{},{},{},[{}],[{}],{},?({}),[{}],[{}]".format(
                    #     tick["mid"], self.o, self.A, self.K, self.r, self.S, self.order_bid, self.order_ask, use_time, tmp, new_order_bid, new_order_ask))

                    # b_order = self.sendOrder("btc_usdt", 'B', 'O', self.order_bid, 1)
                    if new_order_bid > 0:
                        b_order = self.sendOrder("btc_usdt", 'B', 'O', new_order_bid, 1)
                        self.order_dict[b_order.id] = copy(b_order)
                    # a_order = self.sendOrder("btc_usdt", 'S', 'O', self.order_ask, 1)
                    if new_order_ask > 0:
                        a_order = self.sendOrder("btc_usdt", 'S', 'O', new_order_ask, 1)
                        self.order_dict[a_order.id] = copy(a_order)

                    #self.write_log("[output]: nav:{} pos:{}".format(self.nav, self.pos))

            self.max_bid_cross = 0
            self.max_ask_cross = 0

            self.last_sample_ticker = copy(tick)

        self.count = self.count + 1
        self.last_ticker = copy(tick)

        # self.write_log("[output]: nav:{} pos:{}".format(self.nav, self.pos))

        # self.write_log("[onTick] t:{} {}".format(tick["bid1"], tick["ask1"]))
        # if self.pos == 0:
        #     order = self.sendOrder("btc_usdt", 'B', 'O', tick["ask1"], 1)
        #     self.write_log("[onTick] order:{}".format(order.__dict__))
        # elif self.pos > 0:
        #     order = self.sendOrder("btc_usdt", 'S', 'O', tick["bid1"], 1)
        #     self.write_log("[onTick] order:{}".format(order.__dict__))
        # elif self.pos < 0:
        #     order = self.sendOrder("btc_usdt", 'B', 'O', tick['ask1'], 1)
        #     self.write_log("[onTick] order:{}".format(order.__dict__))

    def OnTrade(self, trd):
        super(AsqMakerStrategy, self).OnTrade(trd)

        self.write_log("{},{} pos:{} nav:{} {} {}".format(
            trd, trd.msg, self.pos, self.nav, self.save_ticker["bid1"], self.save_ticker["ask1"]))
        if trd.id in self.order_dict.keys():
            self.order_dict.pop(trd.id)

    def OnFinish(self):
        super(AsqMakerStrategy, self).OnFinish()

    def FindOrder(self, id):
        super(AsqMakerStrategy, self).FindOrder(id)

    def write_log(self, msg):
        print("{},{}".format(self.datetime, msg))


def run():
    def func1(s):
        s = s.split('/')[-1]
        t = time.strptime(s, "%Y%m%d_%H%M%S.csv")
        timestamp = float(time.mktime(t))
        return timestamp

    trades_dir = "/Users/szh/Documents/data/bitmex/merge/btc_usd_swap/20210123"
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
    strategy = AsqMakerStrategy()
    engine = TickEngine()
    engine.RegisterStrategy(strategy)
    engine.Start(tickers)
    return strategy


run()
