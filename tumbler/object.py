# coding=utf-8

from datetime import datetime, timedelta
import json
import time
from logging import INFO
from copy import copy
from collections import defaultdict

import numpy as np
import pandas as pd

from tumbler.constant import EMPTY_STRING, EMPTY_INT, EMPTY_FLOAT, EMPTY_UNICODE, MAX_PRICE_NUM, Direction, Exchange
from tumbler.constant import Status, StopOrderStatus, MQDataType, TradeType, Interval, MarginMode
from tumbler.function.function import get_vt_key, get_web_display_format_symbol, get_from_vt_key
from tumbler.function.order_math import get_round_order_price, get_system_inside_min_volume
from tumbler.function.alpha_factor import factor_zscore

LIVE_LIMIT_ORDER_CONDITIONS = [Status.SUBMITTING.value, Status.NOTTRADED.value, Status.PARTTRADED.value]
LIVE_STOP_ORDER_CONDITIONS = [StopOrderStatus.WAITING.value]
SYSTEM_TO_MOV_ORDER_FORMAT = {
    Status.SUBMITTING.value: "submitted",
    Status.NOTTRADED.value: "open",
    Status.PARTTRADED.value: "partial",
    Status.ALLTRADED.value: "filled",
    Status.CANCELLED.value: "cancelled",
    Status.REJECTED.value: "rejected"
}
SYSTEM_TO_MOV_DIRECTION_FORMAT = {
    Direction.LONG.value: "buy",
    Direction.SHORT.value: "sell"
}


class StrategyParameter(object):
    def __init__(self, vt_symbol, slippage, size, price_tick, rate):
        self.vt_symbol = vt_symbol
        self.slippage = slippage
        self.size = size
        self.price_tick = price_tick
        self.rate = rate


class MQMsg(object):
    def __init__(self):
        self.mq_type = MQDataType.UNKNOWN_DATA.value
        self.datetime = datetime.now()

    def get_transfer(self):
        j = copy(self.__dict__)
        j["datetime"] = str(self.datetime)
        return j

    def parse_transfer(self):
        if self.datetime and self.datetime != "None":
            if len(self.datetime) > 19:
                self.datetime = datetime.strptime(self.datetime, '%Y-%m-%d %H:%M:%S.%f')
            else:
                self.datetime = datetime.strptime(self.datetime, '%Y-%m-%d %H:%M:%S')

    def get_json_msg(self):
        return json.dumps(self.get_transfer())

    def get_from_json(self, json_data):
        self.__dict__ = json_data
        self.parse_transfer()

    def get_from_json_msg(self, data):
        self.__dict__ = json.loads(data)
        self.parse_transfer()

    def get_mq_msg(self):
        return json.dumps({self.mq_type: self.get_transfer()})

    def get_from_mq_msg(self, data):
        mq_order = json.loads(data)
        self.__dict__ = mq_order[self.mq_type]
        self.parse_transfer()


class MarketTradeData(object):
    """
    MarketTradeData
    """

    def __init__(self):
        """
        Constructor
        """
        self.symbol = EMPTY_STRING  # 合约代码
        self.exchange = EMPTY_STRING  # 交易所代码
        self.vt_symbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码

        self.price = EMPTY_FLOAT  # 交易的市场价格
        self.volume = EMPTY_FLOAT  # 交易的数量
        self.direction = EMPTY_STRING  # 交易的方向


class NewMergeTicker(object):
    def __init__(self):
        self.symbol = EMPTY_STRING
        self.bid_prices = [0.0] * MAX_PRICE_NUM
        self.ask_prices = [0.0] * MAX_PRICE_NUM
        self.bid_volumes = [0.0] * MAX_PRICE_NUM
        self.ask_volumes = [0.0] * MAX_PRICE_NUM
        self.accu_volume = 0
        self.accu_amount = 0
        self.accu_buy_price_volume = 0
        self.accu_sell_price_volume = 0
        self.accu_ask_fill = 0
        self.accu_bid_fill = 0
        self.date = None

    def get_np_array(self):
        ret = np.recarray((1,), dtype=NewMergeTicker.get_np_dtype())
        ret[0]["symbol"] = self.symbol
        ret[0]["accu_volume"] = self.accu_volume
        ret[0]["accu_amount"] = self.accu_amount
        ret[0]["accu_buy_price_volume"] = self.accu_buy_price_volume
        ret[0]["accu_sell_price_volume"] = self.accu_sell_price_volume
        ret[0]["accu_ask_fill"] = self.accu_ask_fill
        ret[0]["accu_bid_fill"] = self.accu_bid_fill
        ret[0]["date"] = self.date
        for i in range(1, 11):
            ret[0]["bid{}".format(i)] = self.bid_prices[i - 1]
            ret[0]["ask{}".format(i)] = self.ask_prices[i - 1]
            ret[0]["bidvol{}".format(i)] = self.bid_volumes[i - 1]
            ret[0]["askvol{}".format(i)] = self.ask_volumes[i - 1]
        return ret

    @staticmethod
    def get_np_dtype():
        return [
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
            ('accu_volume', float),
            ('accu_amount', float),
            ('accu_buy_price_volume', float),
            ('accu_sell_price_volume', float),
            ('accu_ask_fill', float),
            ('accu_bid_fill', float),
            ('date', object),
        ]


class TickData(MQMsg):
    """
    Tick data contains information about:
        * last trade in market
        * orderbook snapshot
        * intraday market statistics.
    """

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(TickData, self).__init__()
        # 代码相关
        self.symbol = EMPTY_STRING  # 合约代码
        self.exchange = EMPTY_STRING  # 交易所代码
        self.vt_symbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码

        self.name = EMPTY_STRING  # 合约名字
        self.gateway_name = EMPTY_STRING  # 网关名

        # 成交数据
        self.last_price = EMPTY_FLOAT  # 最新成交价
        self.last_volume = EMPTY_INT  # 最新成交量
        self.volume = EMPTY_INT  # 今天总成交量

        self.open_interest = EMPTY_FLOAT  # 持仓量

        self.time = EMPTY_STRING  # 时间 11:20:56.5
        self.date = EMPTY_STRING  # 日期 20151009
        self.datetime = None  # python的datetime时间对象

        self.upper_limit = EMPTY_FLOAT  # 涨停价
        self.lower_limit = EMPTY_FLOAT  # 跌停价

        # 五档行情
        self.bid_prices = [0.0] * MAX_PRICE_NUM  # 多挡行情,买价
        self.ask_prices = [0.0] * MAX_PRICE_NUM  # 多挡行情,卖价

        self.bid_volumes = [0.0] * MAX_PRICE_NUM  # 多档行情,买量
        self.ask_volumes = [0.0] * MAX_PRICE_NUM  # 多档行情,卖量

        self.mq_type = MQDataType.TICKER.value

    def get_vt_key(self):
        return get_vt_key(self.symbol, self.exchange)

    @staticmethod
    def make_ticker(price):
        t = TickData()
        t.ask_prices[0] = t.bid_prices[0] = price
        t.bid_volumes[0] = t.ask_volumes[0] = 100000
        t.last_price = price
        return copy(t)

    @staticmethod
    def make_ticker_from_bid_ask(symbol, exchange, bid, ask, vol, _datetime):
        t = TickData()
        t.symbol = symbol
        t.exchange = exchange
        t.gateway_name = exchange
        t.name = symbol.replace('_', '/')
        t.vt_symbol = get_vt_key(symbol, exchange)
        t.bid_prices[0] = bid[0]
        t.ask_prices[0] = ask[0]
        t.bid_volumes[0] = bid[1]
        t.ask_volumes[0] = ask[1]
        t.volume = vol

        t.last_price = bid[0]
        t.datetime = _datetime
        return copy(t)

    def get_depth_unique_val(self):
        num = 0
        for i in range(MAX_PRICE_NUM):
            num += 1 * i * self.bid_prices[i] * self.bid_volumes[i]
            num += (1 + MAX_PRICE_NUM) * i * self.ask_prices[i] * self.ask_volumes[i]
        return num

    def get_dict(self):
        dic = {
            "symbol": self.symbol,
            "exchange": self.exchange
        }
        for i in range(1, 6):
            dic["bp{}".format(i)] = self.bid_prices[i - 1]
            dic["ap{}".format(i)] = self.ask_prices[i - 1]
            dic["av{}".format(i)] = self.ask_volumes[i - 1]
            dic["bv{}".format(i)] = self.bid_volumes[i - 1]
        return dic

    @staticmethod
    def get_columns():
        arr = ["symbol", "exchange"]
        for i in range(1, 6):
            arr.append("bp{}".format(i))
            arr.append("ap{}".format(i))
            arr.append("av{}".format(i))
            arr.append("bv{}".format(i))
        return arr

    def has_depth(self):
        return self.bid_prices[0] > 0 and self.ask_prices[0] > 0

    def merge_depth(self):
        if self.bid_prices[0] > 1e-8:
            min_volume = get_system_inside_min_volume(self.symbol, self.bid_prices[0], self.exchange) * 10
            while self.bid_volumes[1] > 0 and self.bid_volumes[0] < min_volume:
                self.bid_volumes[1] += self.bid_volumes[0]
                self.bid_volumes.pop(0)
                self.bid_prices.pop(0)
                self.bid_prices.append(0)
                self.bid_volumes.append(0)

        if self.ask_prices[0] > 1e-8:
            min_volume = get_system_inside_min_volume(self.symbol, self.ask_prices[0], self.exchange) * 10
            while self.ask_volumes[1] > 0 and self.ask_volumes[0] < min_volume:
                self.ask_volumes[1] += self.ask_volumes[0]
                self.ask_volumes.pop(0)
                self.ask_prices.pop(0)
                self.ask_prices.append(0)
                self.ask_volumes.append(0)

    def compute_date_and_time(self):
        if self.datetime:
            self.date = self.datetime.strftime("%Y-%m-%d")
            self.time = self.datetime.strftime("%H:%M:%S")

    def get_json_msg(self):
        return json.dumps(self.get_transfer())

    def get_from_json(self, json_data):
        self.__dict__ = json_data
        self.parse_transfer()

    def get_from_json_msg(self, data):
        self.__dict__ = json.loads(data)
        self.parse_transfer()

    def get_mq_msg(self):
        return json.dumps({MQDataType.TICKER.value: self.get_transfer()})

    def get_from_mq_msg(self, data):
        mq_order = json.loads(data)
        self.__dict__ = mq_order[MQDataType.TICKER.value]
        self.parse_transfer()

    def get_depth(self):
        bids = []
        for i in range(len(self.bid_prices)):
            if self.bid_prices[i] > 0:
                bids.append((self.bid_prices[i], self.bid_volumes[i]))
        asks = []
        for i in range(len(self.ask_prices)):
            if self.ask_prices[i] > 0:
                asks.append((self.ask_prices[i], self.ask_volumes[i]))
        return copy(bids), copy(asks)

    def get_sum_depth_buy_volume(self):
        volume = 0
        for i in range(len(self.ask_volumes)):
            if self.ask_prices[i] > 0:
                volume += self.ask_volumes[i]
        return volume

    def get_sum_depth_sell_volume(self):
        volume = 0
        for i in range(len(self.bid_volumes)):
            if self.bid_prices[i] > 0:
                volume += self.bid_volumes[i]
        return volume

    def get_depth_exchange(self):
        bids = []
        for i in range(len(self.bid_prices)):
            if self.bid_prices[i] > 0:
                bids.append((self.bid_prices[i], self.bid_volumes[i], self.exchange))
        asks = []
        for i in range(len(self.ask_prices)):
            if self.ask_prices[i] > 0:
                asks.append((self.ask_prices[i], self.ask_volumes[i], self.exchange))
        return copy(bids), copy(asks)


class BBOTickData(MQMsg):
    """
    BBO (Best Bid/Offer) for all symbols
    """

    # ----------------------------------------------------------------------
    def __init__(self):
        super(BBOTickData, self).__init__()
        self.exchange = EMPTY_STRING
        self.symbol_dic = {}  # {"btc_usdt":{"bid":[bid_price, bid_volume], "ask":[ask_price, ask_volume], "vol":0}]
        self.mq_type = MQDataType.BBO_TICKER.value
        self.datetime = None

    @staticmethod
    def get_exchange_bbo(exchange):
        return get_vt_key(exchange, MQDataType.BBO_TICKER.value)

    def get_ticker(self, symbol):
        dic = self.symbol_dic.get(symbol, {})
        if dic:
            return TickData.make_ticker_from_bid_ask(symbol, self.exchange, dic["bid"],
                                                     dic["ask"], dic["vol"], dic.get("datetime", self.datetime))

    def get_tickers(self):
        arr = []
        for symbol, dic in self.symbol_dic.items():
            if dic and "bid" in dic.keys() and "ask" in dic.keys() and dic["bid"] and dic["ask"]:
                arr.append(TickData.make_ticker_from_bid_ask(symbol, self.exchange, dic["bid"],
                                                             dic["ask"], dic["vol"],
                                                             dic.get("datetime", self.datetime)))
        return arr

    @staticmethod
    def get_from_ticks_dic(tick_dict, recent_tick_times_require=10):
        require_datetime_min = datetime.now() - timedelta(seconds=recent_tick_times_require)
        bbo_ticker = BBOTickData()
        for vt_symbol, tick in tick_dict.items():
            if tick.datetime >= require_datetime_min:
                bbo_ticker.symbol_dic[tick.symbol] = {
                    "bid": [tick.bid_prices[0], tick.bid_volumes[0]],
                    "ask": [tick.ask_prices[0], tick.ask_volumes[0]]
                }
        bbo_ticker.datetime = datetime.now()
        return bbo_ticker


class MergeTickData(MQMsg):
    """
    Different exchanges' tick merge data.
    """

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(MergeTickData, self).__init__()
        self.vt_symbol = EMPTY_STRING  # vt系统代码
        self.symbol = EMPTY_STRING  # 代码

        self.bids = [(0.0, 0.0, "")] * MAX_PRICE_NUM  # [(bid_price,bid_volume,exchange)]
        self.asks = [(0.0, 0.0, "")] * MAX_PRICE_NUM  # [(ask_price,ask_volume,exchange)]

        self.datetime = None  # 时间戳
        self.mq_type = MQDataType.MERGE_TICKER.value

    def get_depth(self):
        return copy(self.bids), copy(self.asks)


class FutureSpotSpread(MergeTickData):
    """
    FutureSpotSpread
    """

    def __init__(self):
        super(FutureSpotSpread, self).__init__()
        self.mq_type = MQDataType.FUTURE_SPOT_SPREAD.value
        self.spread = 0

    def get_from_merge_tick(self, merge_tick):
        self.vt_symbol = merge_tick.vt_symbol
        self.symbol = merge_tick.symbol

        self.bids = copy(merge_tick.bids)
        self.asks = copy(merge_tick.asks)

        self.datetime = merge_tick.datetime

        if self.bids[0] and self.asks[0]:
            self.spread = self.bids[0] / self.asks[0] - 1


class FundamentalData(object):
    def __init__(self):
        self.asset = EMPTY_STRING
        self.name = EMPTY_STRING
        self.symbol = EMPTY_STRING
        self.chain = EMPTY_STRING
        self.exchange = EMPTY_STRING
        self.max_supply = EMPTY_FLOAT
        self.tags = []

    @staticmethod
    def init_from_mysql_db(arr):
        # asset, name, chain, exchange, max_supply, tags
        fd = FundamentalData()
        fd.asset = arr[0].lower()
        fd.symbol = fd.asset + "_usdt"
        fd.name = arr[1]
        fd.chain = arr[2]
        fd.exchange = arr[3]
        fd.max_supply = arr[4]
        fd.tags = json.loads(arr[5])
        return fd

    @staticmethod
    def get_pandas_from_fd_arr(fd_arr):
        dic = {"asset": [], "symbol": [], "name": [], "chain": [], "exchange": [], "max_supply": [], "tags": []}
        for fd in fd_arr:
            dic["asset"].append(fd.asset)
            dic["symbol"].append(fd.symbol)
            dic["name"].append(fd.name)
            dic["chain"].append(fd.chain)
            dic["exchange"].append(fd.exchange)
            dic["max_supply"].append(fd.max_supply)
            dic["tags"].append(fd.tags)
        return pd.DataFrame(dic)


class FactorData(object):
    def __init__(self):
        self.factor_code = EMPTY_STRING
        self.symbol = EMPTY_STRING
        self.interval = EMPTY_STRING

        self.datetime = None
        self.val = EMPTY_FLOAT

    def __eq__(self, other):
        return self.factor_code == other.factor_code and self.symbol == other.symbol and self.datetime == other.datetime

    def __le__(self, other):
        if self.factor_code == other.factor_code:
            if self.datetime == other.datetime:
                return self.symbol < other.symbol
            else:
                return self.datetime < other.datetime
        else:
            return self.factor_code < other.factor_code

    def __gt__(self, other):
        if self.factor_code == other.factor_code:
            if self.datetime == other.datetime:
                return self.symbol > other.symbol
            else:
                return self.datetime > other.datetime
        else:
            return self.factor_code > other.factor_code

    @staticmethod
    def init_from_mysql_db(arr):
        # `factor_code`, `symbol`, `datetime`, `val`
        fac = FactorData()
        fac.factor_code = arr[0]
        fac.symbol = arr[1]
        fac.datetime = datetime.strptime(arr[2], '%Y-%m-%d %H:%M:%S')
        fac.val = arr[3]
        return fac

    @staticmethod
    def get_factor_df(factor_ret, factor_code):
        factor_ret = [fac for fac in factor_ret if fac.factor_code == factor_code]
        dic = {"symbol": [], "datetime": [], factor_code: []}
        for fac in factor_ret:
            dic["symbol"].append(fac.symbol)
            dic["datetime"].append(fac.datetime)
            dic[factor_code].append(fac.val)
        return pd.DataFrame(dic)

    @staticmethod
    def make_alphalen_factor_df(factor_arr, zscore=True):
        index_array = []
        val_array = []
        for factor_data in factor_arr:
            index_array.append((factor_data.datetime, factor_data.symbol))
            val_array.append(factor_data.val)
        index = pd.MultiIndex.from_tuples(index_array, names=['date', 'symbol'])
        df = pd.DataFrame(val_array, index=index).sort_index()
        if zscore:
            df = factor_zscore(df)
        return df

    @staticmethod
    def suffix_filter(factor_arr, suffix="_usdt"):
        return [fac for fac in factor_arr if
                fac.symbol.endswith(suffix) and not fac.symbol.endswith("down_usdt")
                and not fac.symbol.endswith("up_usdt") and not fac.symbol.endswith("bear_usdt")]

    @staticmethod
    def get_data_from_series(col, series):
        df = pd.DataFrame(series).reset_index()
        return list(zip([col] * len(df.index), df.symbol, df.datetime, df[col]))

    @staticmethod
    def get_diff_symbol_from_ret(ret):
        diff_ret_dic = defaultdict(list)
        for factor_code, symbol, d, val in ret:
            diff_ret_dic[symbol].append([factor_code, symbol, str(d)[:19], val])
        return diff_ret_dic


class BarData(object):
    """
    Candlestick bar data of a certain trading period.
    """

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""

        self.vt_symbol = EMPTY_STRING  # vt系统代码
        self.symbol = EMPTY_STRING  # 代码
        self.exchange = EMPTY_STRING  # 交易所

        self.open_price = EMPTY_FLOAT  # OHLC
        self.high_price = EMPTY_FLOAT
        self.low_price = EMPTY_FLOAT
        self.close_price = EMPTY_FLOAT

        self.date = EMPTY_STRING  # bar开始的时间，日期
        self.time = EMPTY_STRING  # 时间
        self.datetime = None  # python的datetime时间对象

        self.volume = EMPTY_FLOAT  # 成交量
        self.open_interest = EMPTY_INT  # 持仓量

        self.interval = None  # 时间周期

        self.gateway_name = EMPTY_STRING  # gateway

    def before_middle_time(self):
        if self.datetime.hour < 12:
            return True
        else:
            return False

    def after_middle_time(self):
        return not self.before_middle_time()

    @staticmethod
    def load_file_data(filename="", max_size=None):
        ret = []
        i = 0
        f = open(filename, "r")
        for line in f:
            i = i + 1
            try:
                arr = line.strip().split(',')
                if len(arr) == 8:
                    symbol, exchange, _datetime, open_price, high_price, low_price, close_price, volume = arr
                else:
                    symbol, _datetime, open_price, high_price, low_price, close_price, volume = arr
                    exchange = Exchange.BINANCE.value

                bar = BarData()
                bar.symbol = symbol
                bar.exchange = exchange
                bar.vt_symbol = get_vt_key(bar.symbol, bar.exchange)
                bar.open_price = float(open_price)
                bar.high_price = float(high_price)
                bar.low_price = float(low_price)
                bar.close_price = float(close_price)
                bar.volume = float(volume)
                bar.datetime = datetime.strptime(_datetime, '%Y-%m-%d %H:%M:%S')
                ret.append(bar)
                if max_size and i >= max_size:
                    break
            except Exception as ex:
                print(ex, line)

        f.close()
        return ret

    @staticmethod
    def init_from_mysql_db(arr):
        # symbol, _datetime, _open_price, high_price, low_price, close_price, volume
        bar = BarData()
        bar.symbol = arr[0]
        bar.exchange = Exchange.BINANCE.value
        bar.vt_symbol = get_vt_key(bar.symbol, bar.exchange)
        bar.open_price = float(arr[2])
        bar.high_price = float(arr[3])
        bar.low_price = float(arr[4])
        bar.close_price = float(arr[5])
        bar.volume = float(arr[6])
        bar.date, bar.time = arr[1].split(' ')
        bar.datetime = datetime.strptime(arr[1], '%Y-%m-%d %H:%M:%S')
        return bar

    @staticmethod
    def make_alphalen_price_df(bars_arr):
        date_arr = list(set([bar.datetime for bar in bars_arr]))
        symbol_arr = list(set([bar.symbol for bar in bars_arr]))
        price_df = pd.DataFrame(np.random.rand(len(date_arr), 1), index=date_arr, columns=["test"])
        for symbol in symbol_arr:
            price_df[symbol] = np.nan

        for bar in bars_arr:
            price_df.loc[bar.datetime, bar.symbol] = bar.close_price

        price_df = price_df.drop(columns=["test"])
        price_df = price_df.sort_index()
        price_df = price_df.fillna(method='ffill', axis=0)
        return price_df

    @staticmethod
    def suffix_filter(bars_arr, suffix="_usdt"):
        return [bar for bar in bars_arr if
                bar.symbol.endswith(suffix) and not bar.symbol.endswith("down_usdt")
                and not bar.symbol.endswith("up_usdt") and not bar.symbol.endswith("bear_usdt")
                and not bar.symbol.endswith("bull_usdt")]

    def __eq__(self, other):
        return self.vt_symbol == other.vt_symbol and self.datetime == other.datetime

    def __le__(self, other):
        if self.datetime == other.datetime:
            return self.vt_symbol < other.vt_symbol
        else:
            return self.datetime < other.datetime

    def __gt__(self, other):
        if self.datetime == other.datetime:
            return self.vt_symbol > other.vt_symbol
        else:
            return self.datetime > other.datetime

    def __cmp__(self, other):
        if self.datetime == other.datetime:
            if self.vt_symbol == other.vt_symbol:
                return 0
            elif self.vt_symbol < other.vt_symbol:
                return -1
            else:
                return 1
        elif self.datetime < other.datetime:
            return -1
        else:
            return 1

    def same(self, bar):
        if not self.get_key() == bar.get_key():
            return False
        if abs(self.close_price - bar.close_price) > bar.close_price * 0.0001:
            return False
        if abs(self.open_price - bar.open_price) > self.open_price * 0.0001:
            return False
        if self.datetime != bar.datetime:
            return False
        if abs(self.high_price - bar.high_price) > self.high_price * 0.0001:
            return False
        if abs(self.low_price - bar.low_price) > self.low_price * 0.0001:
            return False
        if abs(self.volume - bar.volume) > self.volume * 0.0001:
            return False
        return True

    @staticmethod
    def get_columns():
        return ["datetime", "open", "high", "low", "close", "volume"]

    @staticmethod
    def get_multi_index_columns():
        return ["open", "high", "low", "close", "volume"]

    def get_dict(self):
        return {
            "datetime": self.datetime,
            "open": self.open_price,
            "high": self.high_price,
            "low": self.low_price,
            "close": self.close_price,
            "volume": self.volume
        }

    @staticmethod
    def get_dict_from_bars(bars):
        dic = {}
        for bar in bars:
            dic[str(bar.datetime)] = {
                "open": bar.open_price,
                "high": bar.high_price,
                "low": bar.low_price,
                "close": bar.close_price,
                "volume": bar.volume
            }
        return dic

    @staticmethod
    def get_column_line():
        return ",".join(["symbol", "exchange", "date", "open", "high", "low", "close", "volume"])

    def qlib_datetime_format(self, interval):
        if interval == Interval.DAY.value:
            self.datetime = self.datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        elif interval == Interval.HOUR.value:
            self.datetime = self.datetime.replace(minute=0, second=0, microsecond=0)

    def get_key(self):
        return get_vt_key(self.symbol, self.datetime)

    def get_arr(self):
        return [self.symbol, self.exchange, self.datetime, self.open_price,
                self.high_price, self.low_price, self.close_price, self.volume]

    @staticmethod
    def from_bar_array_to_mysql_data(ret):
        new_ret = []
        for bar in ret:
            arr = [bar.symbol, bar.datetime.strftime("%Y-%m-%d %H:%M:%S"), bar.open_price, bar.high_price,
                   bar.low_price,
                   bar.close_price, bar.volume]
            arr = [str(x) for x in arr]
            new_ret.append(arr)
        return new_ret

    @staticmethod
    def change_from_bar_array_to_dict(bars):
        dic = {}
        for bar in bars:
            dic[bar.get_key()] = bar
        return dic

    def get_line(self):
        return ','.join([str(x) for x in self.get_arr()])

    def get_np_array(self, factor_nums=0):
        arr = [self.datetime, self.open_price, self.high_price, self.low_price, self.close_price, self.volume]
        arr.extend([0] * factor_nums)
        u = np.array(arr)
        return u

    def get_unique_index(self):
        return [self.vt_symbol, self.date + " " + self.time]

    def get_np_data(self):
        return np.array([self.open_price, self.high_price, self.low_price, self.close_price, self.volume])

    @staticmethod
    def get_pandas_index(arr):
        x = []
        y = []
        for a, b in arr:
            x.append(a)
            y.append(b)
        return [x, y]

    @staticmethod
    def get_pandas_from_bars(bars_arr):
        dic = {"symbol": [], "exchange": [], "datetime": [], "open": [], "high": [],
               "low": [], "close": [], "volume": []}
        for bar in bars_arr:
            dic["symbol"].append(bar.symbol)
            dic["exchange"].append(bar.exchange)
            dic["datetime"].append(bar.datetime)
            dic["open"].append(bar.open_price)
            dic["high"].append(bar.high_price)
            dic["low"].append(bar.low_price)
            dic["close"].append(bar.close_price)
            dic["volume"].append(bar.volume)
        return pd.DataFrame(dic)

    def compute_date_and_time(self):
        if self.datetime:
            self.date = self.datetime.strftime("%Y-%m-%d")
            self.time = self.datetime.strftime("%H:%M:%S")

    def is_bar_time_right(self):
        """
        有些数据bar 是不整齐的，这里来判断下
        :return: bool
        """
        assert self.datetime is not None, "Error, Bar datetime is None"
        if self.interval == Interval.MINUTE.value:
            return int(self.datetime.second) == 0
        if self.interval == Interval.HOUR.value:
            return int(self.datetime.second) == 0 and int(self.datetime.minute) == 0
        if self.interval == Interval.DAY.value:
            return int(self.datetime.second) == 0 and int(self.datetime.minute) == 0 and int(self.datetime.hour) == 0
        return False


class OrderManager(object):
    """
    某个交易品种所发的 订单管理
    """

    # ----------------------------------------------------------------------
    def __init__(self, father_strategy, vt_symbol, price_tick):
        self.strategy = father_strategy
        self.vt_symbol = vt_symbol
        self.symbol, self.exchange = get_from_vt_key(self.vt_symbol)
        self.order_dict = {}
        self.pos = 0
        self.r_bar = None
        self.price_tick = price_tick

    def get_already_send_volume(self, direction):
        volume = 0
        for vt_order_id, order in self.order_dict.items():
            if order.is_active():
                if order.direction == direction:
                    volume += order.volume - order.traded
        return volume

    def on_bar(self, bar):
        self.r_bar = copy(bar)

    def on_order(self, order):
        self.order_dict[order.vt_order_id] = copy(order)
        if not order.is_active():
            self.order_dict.pop(order.vt_order_id)

    def to_target_pos(self, target_pos):
        buy_volume = self.get_already_send_volume(Direction.LONG.value)
        sell_volume = self.get_already_send_volume(Direction.SHORT.value)

        minus_val = target_pos - self.pos
        if minus_val > 0:
            uu_volume = minus_val - buy_volume
            if uu_volume > 0:
                price = self.r_bar.close_price * 1.005
                price = get_round_order_price(price, self.price_tick)
                list_orders = self.strategy.buy(self.symbol, self.exchange, price, uu_volume)
                for vt_order_id, order in list_orders:
                    self.order_dict[vt_order_id] = order
        elif minus_val < 0:
            uu_volume = minus_val + sell_volume
            if uu_volume < 0:
                price = self.r_bar.close_price * 0.995
                price = get_round_order_price(price, self.price_tick)
                list_orders = self.strategy.sell(self.symbol, self.exchange, price, abs(uu_volume))
                for vt_order_id, order in list_orders:
                    self.order_dict[vt_order_id] = order

    def to_target_amount(self, amount):
        pass


class OrderData(MQMsg):
    """
    Order data contains information for tracking lastest status
    of a specific order.
    """

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(OrderData, self).__init__()
        # 代码编号相关
        self.symbol = EMPTY_STRING  # 合约代码
        self.exchange = EMPTY_STRING  # 交易所代码
        self.vt_symbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码

        self.order_id = EMPTY_STRING  # 订单编号
        self.vt_order_id = EMPTY_STRING  # 订单在vt系统中的唯一编号，通常是 Gateway名.订单编号
        self.client_id = EMPTY_STRING  # 订单附加信息

        # 报单相关
        self.direction = EMPTY_UNICODE  # 报单方向
        self.type = EMPTY_STRING  # LIMIT单还是MARKET单
        self.offset = EMPTY_UNICODE  # 报单开平仓
        self.price = EMPTY_FLOAT  # 报单价格
        self.deal_price = EMPTY_FLOAT  # 成交价格
        self.volume = EMPTY_INT  # 报单总数量
        self.traded = EMPTY_INT  # 报单成交数量
        self.status = Status.SUBMITTING.value  # 报单状态

        self.order_time = EMPTY_STRING  # 发单时间
        self.cancel_time = EMPTY_STRING  # 撤单时间

        self.gateway_name = EMPTY_STRING  # gateway

        self.mq_type = MQDataType.ORDER.value

    def is_active(self):
        return self.status in LIVE_LIMIT_ORDER_CONDITIONS

    def create_cancel_request(self):
        req = CancelRequest()
        req.symbol = self.symbol
        req.exchange = self.exchange
        req.vt_symbol = self.vt_symbol

        req.order_id = self.order_id
        req.vt_order_id = self.vt_order_id

        return req

    @staticmethod
    def make_reject_order(order_id, symbol, exchange, gateway_name):
        order = OrderData()
        order.symbol = symbol
        order.exchange = exchange
        order.order_id = order_id
        order.vt_order_id = get_vt_key(str(order.order_id), order.exchange)
        order.vt_symbol = get_vt_key(order.symbol, order.exchange)
        order.status = Status.REJECTED.value
        order.gateway_name = gateway_name
        return copy(order)

    def make_reject_cover_order_req(self):
        req = RejectCoverOrderRequest()
        req.symbol = self.symbol
        req.vt_symbol = self.vt_symbol
        req.exchange = self.exchange
        req.price = self.price
        req.volume = self.volume
        return copy(req)

    def make_trade_data(self, trade_id, trade_volume, trade_type=TradeType.EMPTY.value):
        trade = TradeData()
        trade.exchange = self.exchange
        trade.symbol = self.symbol
        trade.vt_symbol = self.vt_symbol

        trade.trade_id = trade.symbol + str(trade_id)  # 成交编号
        trade.vt_trade_id = get_vt_key(trade.trade_id, self.exchange)

        trade.order_id = self.order_id
        trade.vt_order_id = self.vt_order_id

        # 成交相关
        trade.direction = self.direction
        trade.offset = self.offset
        trade.price = self.price
        trade.volume = trade_volume
        trade.trade_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        trade.datetime = datetime.now()
        trade.gateway_name = self.gateway_name
        trade.trade_type = trade_type

        return copy(trade)

    def web_fromat(self):
        return {
            "symbol": get_web_display_format_symbol(self.symbol),
            "side": SYSTEM_TO_MOV_DIRECTION_FORMAT.get(self.direction, ""),
            "order_id": self.vt_order_id,
            "open_price": self.price,
            "deal_price": self.deal_price,
            "amount": self.volume,
            "filled_amount": self.traded,
            "status": SYSTEM_TO_MOV_ORDER_FORMAT.get(self.status, ""),
            "order_time": self.order_time,
            "cancel_time": self.cancel_time
        }


class StopOrder(object):

    def __init__(self):
        self.symbol = EMPTY_STRING
        self.exchange = EMPTY_STRING
        self.vt_symbol = EMPTY_STRING
        self.direction = EMPTY_STRING
        self.offset = EMPTY_STRING
        self.price = EMPTY_FLOAT
        self.volume = EMPTY_FLOAT
        self.vt_order_id = EMPTY_STRING
        self.strategy_name = EMPTY_STRING
        self.vt_order_ids = []
        self.status = StopOrderStatus.WAITING.value

    def is_active(self):
        return self.status in LIVE_STOP_ORDER_CONDITIONS


class TradeData(MQMsg):
    """
    Trade data contains information of a fill of an order. One order
    can have several trade fills.
    """

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(TradeData, self).__init__()

        # 代码编号相关
        self.symbol = EMPTY_STRING  # 合约代码
        self.exchange = EMPTY_STRING  # 交易所代码
        self.vt_symbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码

        self.trade_id = EMPTY_STRING  # 成交编号
        self.vt_trade_id = EMPTY_STRING  # 成交在vt系统中的唯一编号，通常是 Gateway名.成交编号

        self.order_id = EMPTY_STRING  # 订单编号
        self.vt_order_id = EMPTY_STRING  # 订单在vt系统中的唯一编号，通常是 Gateway名.订单编号

        # 成交相关
        self.direction = EMPTY_UNICODE  # 成交方向
        self.offset = EMPTY_UNICODE  # 成交开平仓
        self.price = EMPTY_FLOAT  # 成交价格
        self.volume = EMPTY_INT  # 成交数量
        self.trade_time = EMPTY_STRING  # 成交时间
        self.datetime = None  # 成交时间
        self.gateway_name = EMPTY_STRING  # 网关

        self.trade_type = TradeType.EMPTY.value

        self.mq_type = MQDataType.TRADE_DATA.value


class TradeDataLog(object):
    """
    Trade data contains information of a fill of an order. One order
    can have several trade fills.
    """

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""

        # 代码编号相关
        self.symbol = EMPTY_STRING  # 合约代码
        self.exchange = EMPTY_STRING  # 交易所代码
        self.vt_symbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码

        self.trade_id = EMPTY_STRING  # 成交编号
        self.vt_trade_id = EMPTY_STRING  # 成交在vt系统中的唯一编号，通常是 Gateway名.成交编号

        self.order_id = EMPTY_STRING  # 订单编号
        self.vt_order_id = EMPTY_STRING  # 订单在vt系统中的唯一编号，通常是 Gateway名.订单编号

        # 成交相关
        self.direction = EMPTY_UNICODE  # 成交方向
        self.offset = EMPTY_UNICODE  # 成交开平仓
        self.price = EMPTY_FLOAT  # 成交价格
        self.volume = EMPTY_INT  # 成交数量
        self.trade_time = EMPTY_STRING  # 成交时间
        self.datetime = None  # 成交时间
        self.gateway_name = EMPTY_STRING  # 网关

        self.strategy_name = EMPTY_STRING  # 运行策略名

    def get_txt_msg(self):
        msg = '{}:[{}],{},{},{},{},{},{},{}'.format(self.trade_time, self.strategy_name, self.vt_order_id,
                                                    self.vt_symbol,
                                                    self.direction, self.offset, self.price, self.volume,
                                                    self.gateway_name)
        return msg


class PositionData(MQMsg):
    """
    Positon data is used for tracking each individual position holding.
    """

    # ----------------------------------------------------------------------
    def __init__(self):
        super(PositionData, self).__init__()
        # 代码编号相关
        self.symbol = EMPTY_STRING  # 合约代码
        self.exchange = EMPTY_STRING  # 交易所代码
        self.vt_symbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，合约代码.交易所代码

        # 持仓相关
        self.direction = EMPTY_STRING  # 持仓方向
        self.position = EMPTY_INT  # 持仓量
        self.frozen = EMPTY_INT  # 冻结数量
        self.price = EMPTY_FLOAT  # 持仓均价
        self.vt_position_id = EMPTY_STRING  # 持仓在vt系统中的唯一代码，通常是vtSymbol.方向

        self.mq_type = MQDataType.POSITION.value


class AccountData(MQMsg):
    """
    Account data contains information about balance, frozen and
    available.
    """

    # ----------------------------------------------------------------------
    def __init__(self):
        super(AccountData, self).__init__()

        # 账号代码相关
        self.account_id = EMPTY_STRING  # 账户代码
        self.vt_account_id = EMPTY_STRING  # 账户在vt中的唯一代码，通常是 账户代码.Gateway名

        self.api_key = EMPTY_STRING

        # 数值相关
        self.balance = EMPTY_FLOAT  # 账户净值
        self.frozen = EMPTY_FLOAT  # 冻结金额
        self.available = EMPTY_FLOAT  # 可用资金
        self.level_rate = EMPTY_FLOAT  # 当前仓位杠杆，OKEX5
        self.gateway_name = EMPTY_STRING  # 网关

        self.mq_type = MQDataType.ACCOUNT.value


class DictAccountData(MQMsg):
    def __init__(self):
        super(DictAccountData, self).__init__()
        self.account_name = EMPTY_STRING
        self.account_dict = {}  # {"btc.MOV":{"frozen":100,"balance":200}}
        self.mq_type = MQDataType.DICT_ACCOUNT.value

    def init_from_balance_arr(self, balance_arr, account_name=EMPTY_STRING):
        self.account_dict = {}
        for dic in balance_arr:
            self.account_dict[dic.vt_account_id] = {"frozen": float(dic.frozen), "balance": float(dic.balance)}
        self.account_name = account_name


class LogData(object):
    """
    Log data is used for recording log messages on GUI or in log files.
    """

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.time = time.strftime('%X', time.localtime())  # 日志生成时间
        self.msg = EMPTY_UNICODE  # 日志信息
        self.level = INFO  # 日志级别
        self.gateway_name = EMPTY_STRING  # gateway


class ContractData(MQMsg):
    """
    Contract data contains basic information about each contract traded.
    """

    # ----------------------------------------------------------------------
    def __init__(self):
        super(ContractData, self).__init__()
        """Constructor"""
        self.symbol = EMPTY_STRING  # 代码
        self.exchange = EMPTY_STRING  # 交易所代码
        self.vt_symbol = EMPTY_STRING  # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码
        self.name = EMPTY_UNICODE  # 合约中文名

        self.size = EMPTY_INT  # 合约大小
        self.price_tick = EMPTY_FLOAT  # 合约最小价格TICK
        self.volume_tick = EMPTY_FLOAT  # 合约最小数量tick

        self.min_volume = 1  # 最小交易数量
        self.stop_supported = False  # 是否支持stop order

        self.product = EMPTY_STRING  # 品种种类

        self.support_margin_mode = MarginMode.ALL.value  # 永续USDT的种类，是否支持全仓

        self.delivery_datetime = None  # 发行截止时间
        self.listing_datetime = None  # 发行日期
        self.contract_type = EMPTY_STRING  # 交割合约的种类，ok是alias
        self.mq_type = MQDataType.CONTRACT_DATA.value

    @staticmethod
    def change_from_arr_to_dic(contract_arr):
        dic = {}
        for contract in contract_arr:
            dic[contract.vt_symbol] = contract
        return dic

    def get_contract_val(self, fixed):
        return self.size * fixed

    def is_reverse(self):
        '''
        是否是 颠倒合约，reverse->btc_usd, not reverse->btc_usdt
        '''
        if self.contract_type:
            return not ('_usdt' in self.symbol)
        else:
            return False

    def get_contract_base_symbol(self):
        '''
        获得合约交易的前缀
        '''
        return '_'.join(self.symbol.split('_')[:-1])

    def is_in_update_contract_time(self, hours=1, minutes=0, seconds=0):
        '''
        判断是否处于更新当周合约 到 下一周合约的时间
        '''
        if self.delivery_datetime:
            now = datetime.now()
            if self.delivery_datetime - timedelta(hours=hours, minutes=minutes, seconds=seconds) \
                    < now < self.delivery_datetime:
                return True
        return False

    def is_in_clear_time(self, hours=1, minutes=0, seconds=0):
        '''
        判断是否处于需要清算所有合约头寸的时间
        '''
        if self.delivery_datetime:
            now = datetime.now()
            if self.delivery_datetime - timedelta(hours=hours, minutes=minutes, seconds=seconds) < now:
                return True
        return False

    def is_in_stop_trading_time(self, hours=1, minutes=0, seconds=0):
        '''
        判断是否处于需要清算所有合约头寸的时间
        '''
        if self.delivery_datetime:
            now = datetime.now()
            if self.delivery_datetime - timedelta(hours=hours, minutes=minutes, seconds=seconds) \
                    < now < self.delivery_datetime:
                return True
        return False


class SubscribeRequest(object):
    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.symbol = EMPTY_STRING  # 代码
        self.exchange = EMPTY_STRING  # 交易所

        self.vt_symbol = EMPTY_STRING  # vt系统代码


class UnSubscribeRequest(object):
    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.symbol = EMPTY_STRING  # 代码
        self.exchange = EMPTY_STRING  # 交易所

        self.vt_symbol = EMPTY_STRING  # vt系统代码


class MQSubscribeRequest(SubscribeRequest):
    def __init__(self):
        super(MQSubscribeRequest, self).__init__()

        self.account_name = EMPTY_STRING
        self.subscribe_type = EMPTY_STRING
        self.unique = EMPTY_STRING

    def get_key(self):
        return get_vt_key(get_vt_key(self.vt_symbol, self.subscribe_type), self.account_name)

    def get_json_msg(self):
        c = self.__dict__
        return json.dumps(c)

    def get_from_json(self, js):
        self.symbol = js["symbol"]
        self.exchange = js["exchange"]
        self.vt_symbol = get_vt_key(self.symbol, self.exchange)

        self.account_name = js["account_name"]

    def get_from_json_msg(self, data):
        self.__dict__ = json.loads(data)


class CoverOrderRequest(MQMsg):
    def __init__(self):
        super(CoverOrderRequest, self).__init__()
        self.vt_symbol = EMPTY_STRING
        self.exchange = EMPTY_STRING
        self.direction = EMPTY_STRING
        self.symbol = EMPTY_STRING
        self.price = EMPTY_FLOAT
        self.volume = EMPTY_FLOAT

        self.mq_type = MQDataType.COVER_ORDER_REQUEST.value


class RejectCoverOrderRequest(MQMsg):
    def __init__(self):
        super(RejectCoverOrderRequest, self).__init__()
        self.symbol = EMPTY_STRING
        self.vt_symbol = EMPTY_STRING
        self.direction = EMPTY_STRING
        self.exchange = EMPTY_STRING
        self.price = EMPTY_FLOAT
        self.volume = EMPTY_FLOAT

        self.mq_type = MQDataType

    def make_cover_order_req(self):
        req = CoverOrderRequest()
        req.symbol = self.symbol
        req.vt_symbol = self.vt_symbol
        req.direction = self.direction
        req.exchange = self.exchange
        req.price = self.price
        req.volume = self.volume
        return copy(req)


class OrderRequest(object):
    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.symbol = EMPTY_STRING  # 代码
        self.exchange = EMPTY_STRING  # 交易所
        self.vt_symbol = EMPTY_STRING  # VT合约代码

        self.price = EMPTY_FLOAT  # 价格
        self.volume = EMPTY_INT  # 数量

        self.type = EMPTY_STRING  # 价格类型
        self.direction = EMPTY_STRING  # 买卖
        self.offset = EMPTY_STRING  # 开平

    def create_order_data(self, local_order_id, gateway_name):
        order = OrderData()
        order.symbol = self.symbol
        order.exchange = self.exchange
        order.vt_symbol = self.vt_symbol

        order.order_id = local_order_id
        order.vt_order_id = get_vt_key(order.order_id, gateway_name)

        order.direction = self.direction
        order.type = self.type
        order.offset = self.offset

        order.price = self.price
        order.volume = self.volume
        order.gateway_name = gateway_name

        order.status = Status.SUBMITTING.value

        order.order_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return order


class CancelRequest:
    # ----------------------------------------------------------------------
    def __init__(self):
        self.symbol = EMPTY_STRING  # 代码
        self.exchange = EMPTY_STRING  # 交易所
        self.vt_symbol = EMPTY_STRING  # VT合约代码

        self.order_id = EMPTY_STRING  # 报单号
        self.vt_order_id = EMPTY_STRING  # vt 唯一单号


class FlashCancelRequest(CancelRequest):
    def __init__(self):
        super(FlashCancelRequest, self).__init__()
        self.direction = EMPTY_STRING


class TransferRequest(MQMsg):
    def __init__(self):
        super(TransferRequest, self).__init__()
        self.from_exchange = EMPTY_STRING  # 从哪个交易所转账
        self.to_exchange = EMPTY_STRING  # 转到哪个交易所
        self.from_address = EMPTY_STRING  # 从哪个地址转
        self.from_strategy_name = EMPTY_STRING  # 从哪个运行策略文件名转，唯一性，防止重复转账
        self.to_address = EMPTY_STRING  # 转到哪个地址
        self.asset_id = EMPTY_STRING  # 要转移的资产名
        self.transfer_amount = EMPTY_FLOAT  # 要转移的资产
        self.should_has_min_amount = EMPTY_FLOAT  # 该账户需要的最少资产
        self.timestamp = EMPTY_FLOAT  # 订单的时间, 1587968681.541428格式

        self.mq_type = MQDataType.TRANSFER.value
