# coding=utf-8

from copy import copy

from tumbler.object import Direction, TickData, ContractData
from tumbler.function import get_vt_key, get_round_order_price
from tumbler.constant import Interval, Exchange, Offset, Status, EMPTY_STRING, EMPTY_FLOAT, Product
from tumbler.function.bar import BarGenerator
from tumbler.apps.market_maker.template import (
    MarketMakerTemplate,
)


class PoolInfoReq(object):
    def __init__(self):
        self.pool_id = EMPTY_STRING
        self.direction = EMPTY_STRING
        self.x_volume = EMPTY_FLOAT
        self.y_volume = EMPTY_FLOAT


class PoolInfo(object):
    def __init__(self, strategy, pool_id, vol_x, vol_y):
        self.x = vol_x
        self.y = vol_y
        self.k = vol_x * vol_y
        self.strategy = strategy
        self.pool_id = pool_id

    def query_inside_price(self):
        try:
            return self.y * 1.0 / self.x
        except Exception as ex:
            self.strategy.write_log("[PoolInfo] ex:{}".format(ex))
            return 0.0

    def get_order(self, price):
        xx = self.y / price - self.x
        yy = abs(xx / price)
        if xx > 0:
            return self.pool_id, Direction.LONG.value, abs(xx), abs(yy)
        else:
            return self.pool_id, Direction.SHORT.value, abs(xx), abs(yy)

    def on_pool_trade(self, direction, price, volume):
        if direction == Direction.LONG.value:
            self.x += volume
            self.y -= price * volume
        else:
            self.x -= volume
            self.y += price * volume

        self.strategy.write_log("[PoolInfo] id:{} k:{} x*y:{} x:{} y:{}".format(
            self.pool_id, self.k, self.x*self.y, self.x, self.y))


class AMAMakerV1Strategy(MarketMakerTemplate):
    """
    单市场纯网格AMA 策略。

    运用AMA 的库存管理逻辑，保证 A * B 币始终是一个常数项

    假设A的数量是x, B的数量是y， x * y = k ，交易对是A/B

    可以得到公式 挂单量 xx = y / price - x   (y 表示在他上面的订单成交后的挂单)
            变化y的量: yy = xx * price

    因而可以得到这样一个策略， 假设挂单间距d， 挂单盈利 p%, 挂单数量u, t秒刷新一次的策略。

    只要最终价格回到原点， 基本上就是赚钱的。

    不过有一个问题， 部分成交情况出现的话，会导致数量发生偏移， 这时候需要加一个小的回补单，补到恒定乘积 (部分成交部分的撤销数量,市价处理)

    ----->

    优化一下， 改成可以同时运行多个池子， 一个池子同时只有一个买单 或者一个卖单， 这样就能生成批量订单了。

    要保证 y / x 作为价格，尽量贴近市价

    可以对池子内部价格进行下排序， 然后就能挂5个买单， 5个卖单了。 按排序结果进行挂单

    如果单子部分成交，那么就简单回补一下。

    可以弄成10个池子， 5个买单池，5个卖单池。  这样就能5个买单，5个卖单了。 (1，2池子一个如果是买单了，后面另一个只能是卖单)

    """
    author = "ipqhjjybj"
    class_name = "AMAMakerV1Strategy"

    fee_rate = 0.1
    profit_rate = 0.3
    inc_spread = 0.1
    cover_inc_rate = 0.4
    protect_rate = 10

    protect_buy_price = 0
    protect_sell_price = 0

    backtesting = False
    symbol = "btc_usdt"
    exchange = Exchange.OKEX.value
    contract = None

    # 池子初始的资金配置
    pool_setting_list = [
        ("p1", 1, 1000),
        ("p2", 1, 1000),
        ("p3", 1, 1000),
        ("p4", 1, 1000)
    ]

    bar_window = 4
    interval = Interval.MINUTE.value

    parameters = [
        'backtesting',  # 回测
        'strategy_name',  # 策略加载的唯一性名字
        'class_name',  # 类的名字
        'author',  # 作者
        'symbol',  # 交易对
        'exchange',  # 交易所信息
        'pool_setting_list',    # 池子信息
        'profit_rate',   # 盈利率
        'fee_rate',          # 手续费
        'inc_spread',       # 挂单间距
    ]

    # 需要保存的运行时变量
    variables = [
        'inited',
        'trading',
        'pool_setting_list'
    ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(AMAMakerV1Strategy, self).__init__(mm_engine, strategy_name, settings)

        if self.interval == Interval.HOUR.value:
            self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                                   interval=Interval.HOUR.value, quick_minute=1)
        elif self.interval == Interval.MINUTE.value:
            self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                                   interval=Interval.MINUTE.value, quick_minute=0)

        self.order_dict = {}
        self.pool_order_id_dict = {}
        self.cover_order_dict = {}

        self.need_cover_buy_order_volume = 0
        self.need_cover_sell_order_volume = 0

        self.work_vt_symbol = get_vt_key(self.symbol, self.exchange)
        self.pool_dict = {}
        self.save_ticker = None
        for pool_id, x, y in self.pool_setting_list:
            obj = PoolInfo(self, pool_id, x, y)
            self.pool_dict[pool_id] = obj

    def on_start(self):
        self.update_contract()
        self.write_log("{} is now starting".format(self.strategy_name))
        self.put_event()

    def on_stop(self):
        self.write_log("{} is stop".format(self.strategy_name))
        self.put_event()

    def update_contract(self):
        if not self.contract:
            if self.backtesting:
                con = ContractData()
                con.symbol = "btc_usdt"
                con.exchange = "OKEX"
                con.vt_symbol = "btc_usdt.OKEX"
                con.name = "btc_usdt"
                con.size = 10
                con.price_tick = 0.0001
                con.volume_tick = 0
                con.min_volume = 1
                con.product = Product.SPOT.value
                self.contract = copy(con)
            else:
                self.contract = self.get_contract(self.work_vt_symbol)

    def get_live_order_ids(self):
        return list(self.order_dict.keys())

    def cancel_all_live_order(self):
        all_orders_list = list(self.order_dict.items())
        for vt_order_id, order in all_orders_list:
            self.cancel_order(vt_order_id)

    def update_protect_price(self):
        self.protect_buy_price = self.save_ticker.ask_prices[0]
        self.protect_sell_price = self.save_ticker.bid_prices[0]

        self.write_log("protect_buy_price:{} updated!"
                       "protect_sell_price:{} updated!"
                       .format(self.protect_buy_price, self.protect_sell_price))

    def check_buy_price(self, price, protect_price):
        if price - protect_price < protect_price * self.protect_rate / 100.0:
            return True
        else:
            return False

    def check_sell_price(self, price, protect_price):
        if protect_price - price < protect_price * self.protect_rate / 100.0:
            return True
        else:
            return False

    def get_make_cover_volume(self, dic, direction=None):
        already_need_make_cover_target_volume = 0
        already_need_make_cover_base_volume = 0
        already_price_volumes = []
        for key, s_order in dic.items():
            if direction and s_order.direction != direction:
                continue
            volume = s_order.volume - s_order.traded
            already_price_volumes.append((s_order.price, volume, s_order.vt_order_id))
            already_need_make_cover_target_volume += volume
            already_need_make_cover_base_volume += s_order.price * volume
        return already_need_make_cover_target_volume, already_need_make_cover_base_volume, already_price_volumes

    def cover_orders(self):
        insert_orders = []
        already_cover_target_volume, already_cover_base_volume, already_price_volumes \
            = self.get_make_cover_volume(self.cover_order_dict, Direction.LONG.value)
        if self.need_cover_buy_order_volume > already_cover_target_volume + self.contract.min_volume:
            buy_price = self.save_ticker.ask_prices[0] * (1 + self.cover_inc_rate/100.0)
            buy_price = get_round_order_price(buy_price, self.contract.price_tick)

            volume = self.need_cover_buy_order_volume - already_cover_target_volume
            volume = get_round_order_price(volume, self.contract.volume_tick)

            if self.check_buy_price(buy_price, self.protect_buy_price):
                insert_orders.append((Direction.LONG.value, Offset.OPEN.value, buy_price, volume))
            else:
                self.write_log("[protect] buy_price:{} protect_buy_price:{}"
                               .format(buy_price, self.protect_buy_price))

        already_cover_target_volume, already_cover_base_volume, already_price_volumes \
            = self.get_make_cover_volume(self.cover_order_dict, Direction.SHORT.value)
        if self.need_cover_sell_order_volume > already_cover_target_volume + self.contract.min_volume:
            sell_price = self.save_ticker.bid_prices[0] * (1 - self.cover_inc_rate/100.0)
            sell_price = get_round_order_price(sell_price, self.contract.price_tick)

            volume = self.need_cover_sell_order_volume - already_cover_target_volume
            volume = get_round_order_price(volume, self.contract.volume_tick)

            if self.check_sell_price(sell_price, self.protect_sell_price):
                insert_orders.append((Direction.SHORT.value, Offset.OPEN.value, sell_price, volume))
            else:
                self.write_log("[protect] sell_price:{} protect_sell_price:{}"
                               .format(sell_price, self.protect_sell_price))

        for (d, offset, price, volume) in insert_orders:
            ret_orders = self.send_order(self.symbol, self.exchange, d, offset, price, volume)
            for vt_order_id, order in ret_orders:
                self.cover_order_dict[vt_order_id] = copy(order)

    def get_profit_price(self, price, direction):
        if direction == Direction.LONG.value:
            return price * (1 - self.profit_rate/100.0)
        else:
            return price * (1 + self.profit_rate/100.0)

    def put_orders(self):
        # 里面的价格高， 说明y的数量多，这时候应该挂买单。否则挂卖单。排序后二分
        ret = []
        for pool_id, obj in self.pool_dict.items():
            price = obj.query_inside_price()
            ret.append((price, pool_id))
        ret.sort()

        ll = len(ret)
        n = int(ll / 2.0 + 1e-8)
        buy_ret = ret[n:]
        sell_ret = ret[:n]

        need_send_order_list = []
        for i in range(n):
            _, pool_id = buy_ret[i]
            ori_price = (1 - (i * self.inc_spread) / 100.0) * self.save_ticker.bid_prices[0]
            obj = self.pool_dict[pool_id]
            _, direction, xx, yy = obj.get_order(ori_price)

            #price = (1 - (i * self.inc_spread + self.profit_rate) / 100.0) * self.save_ticker.bid_prices[0]
            price = self.get_profit_price(ori_price, direction)
            price = get_round_order_price(price, self.contract.price_tick)
            volume = get_round_order_price(xx, self.contract.volume_tick)
            need_send_order_list.append((pool_id, direction, price, volume))

        for i in range(n):
            _, pool_id = sell_ret[i]
            ori_price = (1 + (i * self.inc_spread) / 100.0) * self.save_ticker.ask_prices[0]
            obj = self.pool_dict[pool_id]
            _, direction, xx, yy = obj.get_order(ori_price)

            #price = (1 + (i * self.inc_spread + self.profit_rate) / 100.0) * self.save_ticker.ask_prices[0]
            price = self.get_profit_price(ori_price, direction)
            price = get_round_order_price(price, self.contract.price_tick)

            volume = get_round_order_price(xx, self.contract.volume_tick)
            need_send_order_list.append((pool_id, direction, price, volume))

        for pool_id, direction, price, volume in need_send_order_list:
            ret_orders = self.send_order(self.symbol, self.exchange, direction, Offset.OPEN.value, price, volume)
            for vt_order_id, order in ret_orders:
                self.order_dict[vt_order_id] = order
                self.pool_order_id_dict[vt_order_id] = pool_id
                self.write_log(
                    '[send order] direction:{}, price:{},volume:{},vt_order_id:{},order_time:{},status:{}'
                        .format(order.direction, order.price, order.volume, order.vt_order_id, order.order_time,
                                order.status))

    def on_tick(self, tick):
        if tick.bid_prices[0]:
            self.save_ticker = copy(tick)
            self.bg.update_tick(tick)

    def on_bar(self, bar):
        if self.backtesting:
            self.save_ticker = TickData.make_ticker(bar.close_price)

        self.cancel_all_live_order()
        self.put_orders()

    def on_window_bar(self, bar):
        pass

    def on_order(self, order):
        """
        Callback of new order data update.
        """
        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

        if order.is_active():
            if not self.trading:
                self.write_log(
                    "[not trading] now is not in trading condition, cancel order:{}".format(order.vt_order_id))
                self.cancel_order(order.vt_order_id)
            else:
                if order.vt_order_id not in self.get_live_order_ids():
                    self.write_log(
                        "[not in live ids] vt_order_id:{} is not in living ids, cancel it!".format(order.vt_order_id))
                    self.cancel_order(order.vt_order_id)

        if order.traded > 1e-12:
            self.write_important_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}".format(
                order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume,
                order.traded))
            self.output_important_log()

        # 提交的订单推送，过滤掉
        if order.status == Status.SUBMITTING.value:
            return

        if order.vt_order_id in self.order_dict.keys():
            pool_id = self.pool_order_id_dict[order.vt_order_id]
            pool_obj = self.pool_dict[pool_id]

            bef_order = self.order_dict[order.vt_order_id]
            new_traded = order.traded - bef_order.traded
            if new_traded >= 0:
                send_new_traded = new_traded * (1 - self.fee_rate / 100.0)
                pool_obj.on_pool_trade(order.direction, order.price, send_new_traded)
                self.order_dict[order.vt_order_id] = copy(order)

            if not order.is_active():
                if 1e-8 < order.traded + 1e-8 < order.volume:
                    new_traded = order.volume - order.traded
                    send_new_traded = new_traded * (1 - self.fee_rate / 100.0)
                    pool_obj.on_pool_trade(order.direction, order.price, send_new_traded)
                    if order.direction == Direction.LONG.value:
                        self.need_cover_buy_order_volume += new_traded
                    else:
                        self.need_cover_sell_order_volume += new_traded

                del self.pool_order_id_dict[order.vt_order_id]
                del self.order_dict[order.vt_order_id]

        elif order.vt_order_id in self.cover_order_dict.keys():
            bef_order = self.cover_order_dict[order.vt_order_id]
            new_traded = order.traded - bef_order.traded
            if new_traded >= 0:
                if order.direction == Direction.LONG.value:
                    self.need_cover_buy_order_volume -= new_traded
                else:
                    self.need_cover_sell_order_volume -= new_traded
                self.order_dict[order.vt_order_id] = copy(order)
            if not order.is_active():
                del self.cover_order_dict[order.vt_order_id]
        else:
            self.write_log("on_order order exchange not found! vt_order_id:{} ".format(order.vt_order_id))

    def on_trade(self, trade):
        pass

    def output_important_log(self):
        pass

