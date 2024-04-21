# coding=utf-8

import time
from copy import copy
import math

from tumbler.apps.market_maker.template import (
    MarketMakerTemplate,
)
from tumbler.constant import MAX_PRICE_NUM, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset
from tumbler.function import get_system_inside_min_volume
from tumbler.constant import Offset


class AsqMakerStrategy(MarketMakerTemplate):
    """
    根据业界经典的 ASQ 模型写的策略

    做市风险偏好 u = 0.001
    当前持仓 q
    做市终止时间 T
    当前时间 t

    需要求解的值
    由公式 𝜆(𝛿) = A exp(−𝑘𝛿) ，泊松分布 𝜆(𝛿) = (𝜆^n)/n! * exp(−𝜆)
    1、k(盘口厚度系数) -->  k由采样，计算盘口差值，通过 求均值得到数学期望，得出 k = np = E(x) = 采样间隔内的均值
    2、o(市场特征部分波动率) --> 先计算log(x1/x2)得到ui, ui得到每日的标准差si(粗略认为这是波动率)，si/sqrt(3600*24)然后换算到采样时间间隔的波动率
    3、A(指令簿击穿概率系数A) --> A由采样，计算盘口差值，大约A = 𝜆 = 采样间隔的均值 ?? 是这个结论吗？(到时列数据看下)

    AS模型
    中间价R 𝑟(𝑠, 𝑡) = 𝑠 − 𝑞𝛾𝜎2(𝑇 − 𝑡) , r = 中间价 - q * u * o * o * (T - t)
    盘口差价 S = 𝛿𝑎 +𝛿𝑏 = 2ln(1+𝛾)+1𝛾𝜎2(𝑇−𝑡)2 ,  S = 2/u * log(1 + u/k) + 0.5 * u * o * o * (T - t)
    卖单报价 r + S/2
    买单报价 r - S/2

    ASQ模e型
    tmp = sqrt(o * o * u / 2.0 / k / A  * (1 + (u / k) ^ (1 + u / k)))
    卖单报价 b = 1.0/u*log(1+u/k)+ (2.0 * q + 1)/2 * tmp
    买单报价 a = 1.0/u*log(1+u/k)- (2.0 * q - 1)/2 * tmp
    """
    author = "ipqhjjybj"
    class_name = "AsqMakerStrategy"

    symbol_pair = "btm_btc"

    take_sample_time = 1  # 采样时间间隔(秒)

    u = 0.001  # 做市风险偏好
    q = 0  # 当前持仓 -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5
    T = 300  # 做市终止时间
    t = 0  # 做市当前的时间
    k = 0  # 盘口厚度系数
    o = 0  # 市场特征部分波动率
    A = 0  # 指令簿击穿概率系数A

    r = 0  # AS模型: 中间价
    s = 0  # AS模型: 盘口差价

    as_ask_price = 0  # AS模型: 卖单报价
    as_bid_price = 0  # AS模型: 买单报价

    asq_ask_price = 0  # ASQ模型: 卖单报价
    asq_bid_price = 0  # ASQ模型: 买单报价

    parameters = []
    # 需要保存的运行时变量
    variables = [
        'inited',
        'trading'
    ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(AsqMakerStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.update_failed = False
        self.update_account_flag = False

        self.exchange_info = settings["exchange_info"]
        self.long_config = settings["long_config"]
        self.short_config = settings["short_config"]

        self.last_take_sample_time = time.time()

        self.max_min_volume = 0

        self.u_bids = [(0.0, 0.0)] * MAX_PRICE_NUM
        self.u_asks = [(0.0, 0.0)] * MAX_PRICE_NUM

        self.cache_len = 1000000
        self.cache_ask_price_arr = []           # 缓存ask订单簿被击穿的价格档位数量
        self.cache_bid_price_arr = []           # 缓存bid订单簿被击穿的价格档位数量

        self.cache_mid_price_arr = []           # 缓存采样的中间价

        self.pre_u_bids = []
        self.pre_u_asks = []

    def on_init(self):
        self.write_log("{} is now initing".format(self.strategy_name))

    def on_start(self):
        self.write_log("{} is now starting".format(self.strategy_name))
        self.put_event()

    def on_stop(self):
        self.write_log("{} is stop".format(self.strategy_name))
        self.put_event()

    def compute_as(self):
        mid_price = (self.u_bids[0][0] + self.u_asks[0][0]) / 2.0
        # T, t 还未知
        if self.u and self.k:
            self.r = mid_price - self.q * self.u * self.o * self.o * (self.T - self.t)
            self.s = 2.0 / self.u * math.log(1 + 1.0 * self.u / self.k) + 0.5 * self.u * self.o * self.o * (self.T - self.t)
            self.as_ask_price = self.r + self.s / 2.0
            self.as_bid_price = self.r - self.s / 2.0

    def compute_asq(self):
        tmp_val = self.o * self.o * self.u / 2.0 / self.k / self.A * (1 + (self.u / self.k) ** (1 + self.u / self.k))
        tmp = math.sqrt(tmp_val)
        if self.u and self.k:
            self.asq_ask_price = 1.0 / self.u * math.log(1 + 1.0 * self.u / self.k) + (2.0 * self.q + 1) / 2.0 * tmp
            self.asq_bid_price = 1.0 / self.u * math.log(1 + 1.0 * self.u / self.k) + (2.0 * self.q + 1) / 2.0 * tmp

    def run_compute_asks(self):
        '''
        需要返回被击穿的价位，以及被击穿的数量
        '''
        cross_ask_price = 0
        cross_ask_volume = 0
        for i in range(1, 6):
            if self.pre_u_asks[i-1][0] < self.u_asks[0][0]:
                cross_ask_price = i * self.exchange_info["price_tick"]
                cross_ask_volume += self.pre_u_asks[i-1][1]
            elif abs(self.pre_u_asks[i-1][0] - self.u_asks[0][0]) < 1e-13 and self.pre_u_asks[i-1][1] > self.u_asks[0][1]:
                cross_ask_price = i * self.exchange_info["price_tick"]
                cross_ask_volume += self.pre_u_asks[i-1][1] - self.u_asks[0][1]
            else:
                break
        return cross_ask_price, cross_ask_volume

    def run_compute_bids(self):
        '''
        需要返回被击穿的价位，以及被击穿的数量
        '''
        cross_bid_price = 0
        cross_bid_volume = 0
        for i in range(1, 6):
            if self.pre_u_bids[i-1][0] > self.u_bids[0][0]:
                cross_bid_price = i * self.exchange_info["price_tick"]
                cross_bid_volume += self.pre_u_bids[i-1][1]
            elif abs(self.pre_u_bids[i-1][0] - self.u_bids[0][0]) < 1e-13 and self.pre_u_bids[i-1][1] > self.u_bids[0][1]:
                cross_bid_price = i * self.exchange_info["price_tick"]
                cross_bid_volume += self.pre_u_bids[i-1][1] - self.u_bids[0][1]
            else:
                break
        return cross_bid_price, cross_bid_volume

    def compute_volatility(self):
        if len(self.cache_mid_price_arr) > 2:
            new_r = []
            for i in range(1, len(self.cache_mid_price_arr)):
                new_r.append(math.log(self.cache_mid_price_arr[i]) - math.log(self.cache_mid_price_arr[i-1]))
            mid_v = sum(new_r) / len(new_r)
            tmp_s = 0
            for v in new_r:
                tmp_s += (v - mid_v) ** 2
            tmp_s /= len(new_r)

            self.o = math.sqrt(tmp_s) / math.sqrt(self.take_sample_time)

    def take_sample(self):
        if self.pre_u_asks and self.pre_u_bids:
            cross_ask_price, cross_ask_volume = self.run_compute_asks()
            cross_bid_price, cross_bid_volume = self.run_compute_bids()

            msg = "cross_ask_price:{} cross_bid_price:{}".format(cross_ask_price, cross_bid_price)
            self.write_log(msg)

            self.cache_ask_price_arr.append(cross_ask_price)
            self.cache_bid_price_arr.append(cross_bid_price)

            if len(self.cache_ask_price_arr) > self.cache_len:
                self.cache_ask_price_arr.pop(0)
            if len(self.cache_bid_price_arr) > self.cache_len:
                self.cache_bid_price_arr.pop(0)

            k_ask = 1.0 * sum(self.cache_bid_price_arr) / len(self.cache_bid_price_arr)
            k_bid = 1.0 * sum(self.cache_ask_price_arr) / len(self.cache_ask_price_arr)

            self.k = (k_ask + k_bid) / 2.0
            self.A = self.k

        mid_price = (self.u_bids[0][0] + self.u_asks[0][0]) / 2.0
        self.cache_mid_price_arr.append(mid_price)
        if len(self.cache_mid_price_arr) > self.cache_len:
            self.cache_mid_price_arr.pop(0)

        self.compute_volatility()
        if self.o:
            self.compute_as()
            self.compute_asq()

        self.pre_u_bids = copy(self.u_bids)
        self.pre_u_asks = copy(self.u_asks)

    def on_tick(self, tick):
        # self.write_log("[on_tick] tick.last_price:{}".format(tick.last_price))
        if tick.bid_prices[0] > 0:
            self.max_min_volume = max(self.max_min_volume,
                                      get_system_inside_min_volume(self.symbol_pair, tick.bid_prices[0],
                                                                   self.exchange_info["exchange_name"]))

            bids, asks = tick.get_depth()
            if bids:
                self.u_bids = copy(bids)
            if asks:
                self.u_asks = copy(asks)

            if self.trading:
                self.t = time.time()
                self.T = 3600 * 24 + self.t
                if self.t - self.last_take_sample_time > self.take_sample_time:
                    self.take_sample()
                    self.output_condition()

    def on_bar(self, bar):
        pass

    def on_order(self, order):
        pass

    def on_trade(self, trade):
        pass

    def output_condition(self):
        msg = "o:{} k:{} A:{}".format(self.o, self.k, self.A)
        self.write_log(msg)
        msg = "as: r:{} s:{} as_ask_price:{} as_bid_price:{}".format(self.r, self.s, self.as_ask_price, self.as_bid_price)
        self.write_log(msg)
        msg = "asq: asq_ask_price:{} asq_bid_price:{}".format(self.asq_ask_price, self.asq_bid_price)
        self.write_log(msg)
