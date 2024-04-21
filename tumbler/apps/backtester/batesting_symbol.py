# coding=utf-8

from copy import copy
from collections import defaultdict
from datetime import datetime

from pandas import DataFrame

from tumbler.function import get_vt_key, get_from_vt_key, get_round_order_price
from tumbler.constant import Status, Direction, StopOrderStatus, OrderType
from tumbler.object import TradeData, OrderData, StopOrder

from .base import BacktestingMode, DailyResult, STOPORDER_PREFIX


class BacktestingSymbol(object):
    def __init__(self, engine, vt_symbol, interval, rate, slippage, size, price_tick, mode):
        self.engine = engine
        self.vt_symbol = vt_symbol
        self.symbol, self.exchange = get_from_vt_key(self.vt_symbol)
        self.mode = mode

        self.rate = rate
        self.slippage = slippage
        self.size = size
        self.price_tick = price_tick

        self.tick = None
        self.bar = None
        self.datetime = None

        self.interval = interval
        self.days = 0
        self.callback = None

        self.stop_order_count = 0
        self.stop_orders = {}
        self.active_stop_orders = {}

        self.limit_order_count = 0
        self.limit_orders = {}
        self.active_limit_orders = {}

        self.trade_count = 0
        self.trades = {}

        self.daily_results = {}
        self.daily_df = None

    def clear_data(self):
        """
        Clear all data of last backtesting.
        """
        self.tick = None
        self.bar = None
        self.datetime = None

        self.stop_order_count = 0
        self.stop_orders.clear()
        self.active_stop_orders.clear()

        self.limit_order_count = 0
        self.limit_orders.clear()
        self.active_limit_orders.clear()

        self.trade_count = 0
        self.trades.clear()

        self.daily_results.clear()

    def calculate_result(self):
        self.engine.output("[vt_symbol]:{} calculate_result".format(self.vt_symbol))

        if not self.trades:
            self.engine.output("[vt_symbol]:{} no tradeds!".format(self.vt_symbol))
            return

        # Add trade data into daily result.
        for trade in self.trades.values():
            d = trade.datetime.date()
            daily_result = self.daily_results[d]
            daily_result.add_trade(trade)

        # Calculate daily result by iteration.
        pre_close = 0
        start_pos = 0

        for daily_result in self.daily_results.values():
            daily_result.calculate_pnl(pre_close, start_pos, self.size, self.rate, self.slippage)

            pre_close = daily_result.close_price
            start_pos = daily_result.end_pos

        # Generate dataframe
        results = defaultdict(list)

        for daily_result in self.daily_results.values():
            for key, value in daily_result.__dict__.items():
                results[key].append(value)

        self.daily_df = DataFrame.from_dict(results).set_index("date")

        self.engine.output("[vt_symbol]:{} end calculate result".format(self.vt_symbol))
        return self.daily_df

    def update_daily_close(self, price):
        d = self.datetime.date()

        daily_result = self.daily_results.get(d, None)
        if daily_result:
            daily_result.close_price = price
        else:
            self.daily_results[d] = DailyResult(d, price)

    def new_bar(self, bar):
        self.bar = bar
        self.datetime = bar.datetime

        self.cross_limit_order()
        self.cross_stop_order()
        self.engine.strategy.on_bar(bar)

        self.update_daily_close(bar.close_price)

    def new_tick(self, tick):
        self.tick = tick
        self.datetime = tick.datetime

        self.cross_limit_order()
        self.cross_stop_order()
        self.engine.strategy.on_tick(tick)

        self.update_daily_close(tick.last_price)

    def cross_limit_order(self):
        """
        Cross limit order with last bar/tick data.
        """
        if self.mode == BacktestingMode.BAR.value:
            long_cross_price = self.bar.low_price
            short_cross_price = self.bar.high_price
            long_best_price = self.bar.open_price
            short_best_price = self.bar.open_price
        else:
            long_cross_price = self.tick.ask_prices[0]
            short_cross_price = self.tick.bid_prices[0]
            long_best_price = long_cross_price
            short_best_price = short_cross_price

        for order in list(self.active_limit_orders.values()):
            # Push order update with status "not traded" (pending).
            is_submitting = False
            if order.status == Status.SUBMITTING.value:
                is_submitting = True
                order.status = Status.NOTTRADED.value
                self.engine.strategy.on_order(order)

            # Check whether limit orders can be filled.
            long_cross = (
                    order.direction == Direction.LONG.value
                    and order.price >= long_cross_price
                    and long_cross_price > 0
            )

            short_cross = (
                    order.direction == Direction.SHORT.value
                    and order.price <= short_cross_price
                    and short_cross_price > 0
            )

            if not long_cross and not short_cross:
                continue

            self.active_limit_orders.pop(order.vt_order_id)

            # Push order udpate with status "all traded" (filled).
            order.traded = order.volume
            order.status = Status.ALLTRADED.value
            self.engine.strategy.on_order(order)

            # Push trade update
            self.trade_count += 1

            trade_price = order.price
            if long_cross:
                if is_submitting:
                    trade_price = min(order.price, long_best_price)
            else:
                if is_submitting:
                    trade_price = max(order.price, short_best_price)

            trade = TradeData()
            trade.symbol = order.symbol
            trade.exchange = order.exchange
            trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)

            trade.order_id = order.order_id
            trade.vt_order_id = order.vt_order_id
            trade.trade_id = str(self.trade_count)
            trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
            trade.direction = order.direction
            trade.offset = order.offset
            trade.price = trade_price
            trade.volume = order.volume
            trade.trade_time = self.datetime.strftime("%Y-%m-%d %H:%M:%S")
            trade.datetime = self.datetime

            self.engine.strategy.on_trade(trade)

            self.trades[trade.vt_trade_id] = trade

    def cross_stop_order(self):
        """
        Cross stop order with last bar/tick data.
        """
        if self.mode == BacktestingMode.BAR.value:
            long_cross_price = self.bar.high_price
            short_cross_price = self.bar.low_price
            long_best_price = self.bar.open_price
            short_best_price = self.bar.open_price
        else:
            long_cross_price = self.tick.last_price
            short_cross_price = self.tick.last_price
            long_best_price = long_cross_price
            short_best_price = short_cross_price

        for stop_order in list(self.active_stop_orders.values()):
            # Check whether stop order can be triggered.
            long_cross = (
                    stop_order.direction == Direction.LONG.value
                    and stop_order.price <= long_cross_price
            )

            short_cross = (
                    stop_order.direction == Direction.SHORT.value
                    and stop_order.price >= short_cross_price
            )

            if not long_cross and not short_cross:
                continue

            # Create order data.
            self.limit_order_count += 1

            order = OrderData()
            order.symbol = stop_order.symbol
            order.exchange = stop_order.exchange
            order.order_id = str(self.limit_order_count)
            order.vt_order_id = get_vt_key(order.order_id, order.exchange)
            order.direction = stop_order.direction
            order.offset = stop_order.offset
            order.price = stop_order.price
            order.volume = stop_order.volume
            order.traded = 0
            order.status = Status.SUBMITTING.value

            order.datetime = self.datetime

            self.limit_orders[order.vt_order_id] = order

            # Create trade data.
            if long_cross:
                trade_price = max(stop_order.price, long_best_price)
            else:
                trade_price = min(stop_order.price, short_best_price)

            self.trade_count += 1

            trade = TradeData()
            trade.symbol = order.symbol
            trade.exchange = order.exchange
            trade.vt_symbol = get_vt_key(trade.symbol, trade.exchange)

            trade.order_id = order.order_id
            trade.vt_order_id = order.vt_order_id
            trade.trade_id = str(self.trade_count)
            trade.vt_trade_id = get_vt_key(trade.trade_id, trade.exchange)
            trade.direction = order.direction
            trade.offset = order.offset
            trade.price = trade_price
            trade.volume = order.volume
            trade.trade_time = self.datetime.strftime("%Y-%m-%d %H:%M:%S")
            trade.datetime = self.datetime

            self.trades[trade.vt_trade_id] = trade

            # Update stop order.
            stop_order.vt_order_ids = [[order.vt_order_id, copy(order)]]
            stop_order.status = StopOrderStatus.TRIGGERED.value

            self.active_stop_orders.pop(stop_order.vt_order_id)

            # Push update to strategy.
            self.engine.strategy.on_stop_order(stop_order)

            order.status = Status.ALLTRADED.value
            order.traded = order.volume
            self.engine.strategy.on_order(order)

            self.engine.strategy.on_trade(trade)

    def send_order(self, strategy, symbol, exchange, direction, offset, price, volume, stop=False, lock=False):
        price = get_round_order_price(price, self.price_tick)
        if stop:
            vt_order_id = self.send_stop_order(symbol, exchange, direction, offset, price, volume)
        else:
            vt_order_id = self.send_limit_order(symbol, exchange, direction, offset, price, volume)

        order_id, exchange = get_from_vt_key(vt_order_id)

        if not stop:
            order = OrderData()
            order.symbol = symbol
            order.exchange = exchange
            order.vt_symbol = get_vt_key(symbol, exchange)
            order.order_id = order_id
            order.vt_order_id = vt_order_id
            order.direction = direction
            order.type = OrderType.LIMIT.value
            order.offset = offset
            order.price = price
            order.volume = volume
            order.order_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            order.status = Status.SUBMITTING.value

            return [[vt_order_id, order]]
        else:
            stop_order = copy(self.stop_orders[vt_order_id])
            return [[vt_order_id, stop_order]]

    def send_stop_order(self, symbol, exchange, direction, offset, price, volume):
        self.stop_order_count += 1

        stop_order = StopOrder()
        stop_order.symbol = symbol
        stop_order.exchange = exchange
        stop_order.vt_symbol = get_vt_key(symbol, exchange)
        stop_order.direction = direction
        stop_order.offset = offset
        stop_order.price = price
        stop_order.volume = volume
        stop_order.vt_order_id = "{}.{}".format(STOPORDER_PREFIX, self.stop_order_count)
        stop_order.strategy_name = self.engine.strategy.strategy_name

        self.active_stop_orders[stop_order.vt_order_id] = copy(stop_order)
        self.stop_orders[stop_order.vt_order_id] = copy(stop_order)

        return stop_order.vt_order_id

    def send_limit_order(self, symbol, exchange, direction, offset, price, volume):
        self.limit_order_count += 1

        order = OrderData()
        order.symbol = symbol
        order.exchange = exchange
        order.vt_symbol = get_vt_key(symbol, exchange)
        order.order_id = str(self.limit_order_count)
        order.vt_order_id = get_vt_key(order.order_id, exchange)
        order.direction = direction
        order.offset = offset
        order.price = price
        order.volume = volume
        order.status = Status.SUBMITTING.value

        order.datetime = self.datetime

        self.active_limit_orders[order.vt_order_id] = copy(order)
        self.limit_orders[order.vt_order_id] = copy(order)

        return order.vt_order_id

    def cancel_order(self, strategy, vt_order_id):
        """
        Cancel order by vt_order_id.
        """
        if vt_order_id.startswith(STOPORDER_PREFIX):
            self.cancel_stop_order(strategy, vt_order_id)
        else:
            self.cancel_limit_order(strategy, vt_order_id)

    def cancel_stop_order(self, strategy, vt_order_id):
        if vt_order_id not in self.active_stop_orders:
            return
        stop_order = self.active_stop_orders.pop(vt_order_id)

        stop_order.status = StopOrderStatus.CANCELLED.value
        self.engine.strategy.on_stop_order(stop_order)

    def cancel_limit_order(self, strategy, vt_order_id):
        if vt_order_id not in self.active_limit_orders:
            return
        order = self.active_limit_orders.pop(vt_order_id)

        order.status = Status.CANCELLED.value
        self.engine.strategy.on_order(order)

    def cancel_all(self, strategy):
        """
        Cancel all orders, both limit and stop.
        """
        vt_order_ids = list(self.active_limit_orders.keys())
        for vt_order_id in vt_order_ids:
            self.cancel_limit_order(strategy, vt_order_id)

        vt_order_ids = list(self.active_stop_orders.keys())
        for vt_order_id in vt_order_ids:
            self.cancel_stop_order(strategy, vt_order_id)

    def get_all_trades(self):
        """
        Return all trade data of current backtesting result.
        """
        return list(self.trades.values())

    def get_all_orders(self):
        """
        Return all limit order data of current backtesting result.
        """
        return list(self.limit_orders.values())

    def get_all_daily_results(self):
        """
        Return all daily result data.
        """
        return list(self.daily_results.values())
