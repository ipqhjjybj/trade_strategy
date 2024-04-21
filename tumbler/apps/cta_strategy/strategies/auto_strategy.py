# coding=utf-8

from copy import copy

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
)
from tumbler.object import BarData
from tumbler.constant import Direction, Status, Interval
from tumbler.function.bar import *
from tumbler.function import get_vt_key, get_round_order_price
import datetime, math, time
from collections import defaultdict


class AutoCtaStrategy(CtaTemplate):
    author = "weirj"
    class_name = "AutoCtaStrategy"

    symbol = "btc_usdt"
    exchange = "HUOBIS"

    parameters = ['strategy_name',
                  'class_name',
                  'author',
                  'symbol_pair',
                  'exchange',
                  'bar_window',
                  'exchange_info',
                  'vt_symbols_subscribe',
                  'lot',
                  ]

    # 需要保存的运行时变量
    variables = ['inited', 'trading']

    def __init__(self, mm_engine, strategy_name, settings):
        super(AutoCtaStrategy, self).__init__(mm_engine, strategy_name, settings)

        self.n = 10
        self.stopPct_ar = np.array([9, 6, 6, 5, 5, 5, 5, 5, 4, 5])
        # self.lot = 1
        print('self.lot=%d' % self.lot)
        self.wait_seconds = 10
        self.max_cancel_num = 8
        self.order_aggr = 0
        self.cancel_dict_times_count = defaultdict(int)

        self.price_tick = self.exchange_info["price_tick"]
        self.price_tick = 0.1
        self.signal_ar = np.zeros(self.n)
        self.targetPos_ar = np.zeros(self.n)
        self.trailPrc_ar = np.zeros(self.n)
        self.stopPrc_ar = np.zeros(self.n)
        self.targetPos = 0
        self.pos = 0
        self.order_dict = {}
        self.order_msg = {}
        self.trading = False
        self.bar_window = 1
        self.lastsendtm = datetime.datetime.now()

        self.bg = BarGenerator(self.on_bar, window=self.bar_window, on_window_bar=self.on_window_bar,
                               interval=Interval.HOUR.value, quick_minute=1)
        self.am = BarArray(10000)

    def on_init(self):
        print('load bars')
        self.load_bar(10000)
        print('load bar finish.')
        self.write_log("on_init")

    def on_start(self):
        print('OnStart checking positions.')
        print('symbolm, exch:', self.symbol_pair, self.exchange)
        long_pos, short_pos = self.cta_engine.get_position(self.symbol_pair, self.exchange)
        self.pos = long_pos.position - short_pos.position
        print('load position from engien, long_pos:%.2f, short_pos:%.2f, pos:%.2f, targetPos:%.2f' % (
        long_pos.position, short_pos.position, self.pos, self.targetPos))
        self.write_log('load position from engien, long_pos:%.2f, short_pos:%.2f, pos:%.2f, targetPos:%.2f' % (
        long_pos.position, short_pos.position, self.pos, self.targetPos))
        for i in range(self.n):
            print('targetPos[%d]:%.1f stopPrc:%.2f trailPrc:%.2f' % (
            i, self.targetPos_ar[i], self.stopPrc_ar[i], self.trailPrc_ar[i]))
            self.write_log('targetPos[%d]:%.1f stopPrc:%.2f trailPrc:%.2f' % (
            i, self.targetPos_ar[i], self.stopPrc_ar[i], self.trailPrc_ar[i]))

        if self.pos != self.targetPos:
            print('error: self.pos[%.4f] != targetPos[%.4f]' % (self.pos, self.targetPos))
        idx = self.am.idx
        dts = self.am.datetime
        for i in range(1, idx):
            dif = dts[i] - dts[i - 1]
            if dif.astype('timedelta64[h]') != np.timedelta64(1, 'h'):
                print('error bar idx=%d gap:%s %s' % (i, dts[i], dts[i - 1]))
                self.write_log('error bar idx=%d gap:%s %s' % (i, dts[i], dts[i - 1]))
        print('check barData finish, last bar %s' % dts[idx])
        self.write_log('OnStart Finish. \ncheck barData finish, last bar %s, idx: %d' % (dts[idx], idx))
        # df = pd.DataFrame(data={'datetime':self.am.datetime,'open':self.am.open,'high': self.am.high, 'low': self.am.low, 'close':self.am.close})
        # df.to_csv('mydata.csv')
        self.trading = True

    def on_stop(self):
        self.write_log("on_stop")

    def updatePos(self, bar):
        lastPrc = bar.close_price
        highPrc = bar.high_price
        lowPrc = bar.low_price
        totalPos = 0
        # toprint = 3
        for i in range(self.n):
            pos = self.targetPos_ar[i]
            sig = self.signal_ar[i]
            if pos > 0:
                if lowPrc > self.trailPrc_ar[i]:
                    self.trailPrc_ar[i] = lowPrc
                    self.stopPrc_ar[i] = self.trailPrc_ar[i] * (1 - self.stopPct_ar[i] * 0.01)
                if lowPrc < self.stopPrc_ar[i]:
                    self.targetPos_ar[i] = 0
                    # if toprint == i:
                    self.write_log('signal_%d long trail stop. exit price:%.2f' % (i, bar.close_price))
                if sig < 0:
                    self.targetPos_ar[i] = 0
            elif pos < 0:
                if highPrc < self.trailPrc_ar[i]:
                    self.trailPrc_ar[i] = highPrc
                    self.stopPrc_ar[i] = self.trailPrc_ar[i] * (1 + self.stopPct_ar[i] * 0.01)
                if highPrc > self.stopPrc_ar[i]:
                    self.targetPos_ar[i] = 0
                    # if toprint == i:
                    self.write_log('signal_%d short trail stop. exit price:%.2f' % (i, bar.close_price))
            elif pos == 0:
                if sig > 0:
                    self.targetPos_ar[i] = 1
                    self.trailPrc_ar[i] = lastPrc
                    self.stopPrc_ar[i] = self.trailPrc_ar[i] * (1 - self.stopPct_ar[i] * 0.01)
                    # if toprint == i:
                    self.write_log('long entry @%.2f stop:%.2f' % (lastPrc, self.stopPrc_ar[i]))
                if sig < 0:
                    self.targetPos_ar[i] = -1
                    self.trailPrc_ar[i] = lastPrc
                    self.stopPrc_ar[i] = self.trailPrc_ar[i] * (1 + self.stopPct_ar[i] * 0.01)
                    # if toprint == i:
                    self.write_log('short entry @%.2f stop:%.2f' % (lastPrc, self.stopPrc_ar[i]))
            totalPos += self.targetPos_ar[i]
        self.targetPos = self.lot * totalPos

    def determinePrice(self, tick, bOrS):
        if bOrS == 'B':
            if self.order_aggr == 0:
                price = tick.bid_prices[0] + 10.0
            elif self.order_aggr == 1:
                price = tick.bid_prices[0] + 20.0
            else:
                price = tick.ask_prices[2]
        elif bOrS == 'S':
            if self.order_aggr == 0:
                price = tick.ask_prices[0] - 10.0
            elif self.order_aggr == 1:
                price = tick.ask_prices[0] - 20.0
            else:
                price = tick.bid_prices[2]
        return price

    def DoTrade(self, tick):
        # return
        if len(self.order_dict) == 0:
            diff = self.targetPos - self.pos
            if math.fabs(diff) > 1e-8:
                tm = datetime.datetime.now()
                if tm - self.lastsendtm < datetime.timedelta(seconds=10):
                    self.write_log(
                        'DoTrd NoPending diff=%.2f targetPos=%.2f pos=%.2f, less than 10 seconds tm:%s lastsendtm:%s' % (
                        diff, self.targetPos, self.pos, tm, self.lastsendtm))
                    return
                if diff > 0:
                    price = self.determinePrice(tick, 'B')
                    list_orders = self.buy(self.symbol_pair, self.exchange, price, diff)
                    for id, order in list_orders:
                        self.order_dict[id] = order
                    self.write_log("DoTrade Send Buy %.2f prc:%.4f" % (diff, price))
                    self.lastsendtm = datetime.datetime.now()
                elif diff < 0:
                    price = self.determinePrice(tick, 'S')
                    list_orders = self.sell(self.symbol_pair, self.exchange, price, abs(diff))
                    for id, order in list_orders:
                        self.order_dict[id] = order
                    self.write_log("DoTrade Send Sell %.2f prc:%.4f" % (-diff, price))
                    self.lastsendtm = datetime.datetime.now()
        else:
            self.try_cancel_pending()

        return

    def try_cancel_pending(self):
        need_cancel_sets = []
        now = time.time()
        for vt_order_id, order in self.order_dict.items():
            if now - order.order_time > self.wait_seconds * (self.cancel_dict_times_count.get(vt_order_id, 0) + 1):
                need_cancel_sets.append(order.vt_order_id)
        if len(need_cancel_sets) > 0:
            self.cancel_sets_order(need_cancel_sets)
            self.write_log("[try_cancel_pending] {} set to cancel!".format(need_cancel_sets))

    def cancel_sets_order(self, need_cancel_sets):
        for vt_order_id in need_cancel_sets:
            self.cancel_order(vt_order_id)
            self.cancel_dict_times_count[vt_order_id] += 1
            if self.cancel_dict_times_count[vt_order_id] > self.max_cancel_num:
                if vt_order_id in self.order_dict.keys():
                    self.cancel_dict_times_count.pop(vt_order_id)
                    order = self.order_dict[vt_order_id]
                    del self.order_dict[vt_order_id]
                    self.write_log("[cancel_sets_order] Cancel Order 5 more times, {} is removed!".format(vt_order_id))

    def on_tick(self, tick):
        self.bg.update_tick(tick)
        if self.trading:
            self.DoTrade(tick)

    def on_bar(self, bar):
        self.bg.update_bar(bar)

    def on_window_bar(self, bar):
        am = self.am
        am.update_bar(bar)
        if am.idx < 1000:
            return

        idx = am.idx
        open1 = am.open[:idx + 1]
        close = am.close[:idx + 1]
        high = am.high[:idx + 1]
        low = am.low[:idx + 1]

        # self.stopPct_ar = np.array([9, 6, 6, 5, 5, 5, 5, 5, 4, 5])

        self.signal_ar[0] = crossup(ma(close, 200), ma(close, 400))[-1]
        self.signal_ar[1] = crossdown(ma(close, 200), ma(close, 500))[-1]
        self.signal_ar[2] = sar_long(high, low, 0.009)[-1]
        self.signal_ar[3] = sar_short(high, low, 0.004)[-1]
        self.signal_ar[4] = crossup(open1, ref(hhv(close, 200), 1))[-1]
        self.signal_ar[5] = crossdown(open1, ref(llv(close, 200), 1))[-1]
        self.signal_ar[6] = crossup(close, bbandup(close, 300, 1))[-1]
        self.signal_ar[7] = crossdown(close, bbanddown(close, 360, 1.0))[-1]

        # 下面两个信号估计有问题
        self.signal_ar[8] = crossup(ht_trend(close), close + 0.25 * atr(high, low, close, 20))[-1]
        self.signal_ar[9] = crossdown(ma(close, 5), ema(close, 300) - 0.5 * atr(high, low, close, 40))[-1]

        self.updatePos(bar)

        if self.trading:
            for i in range(self.n):
                self.write_log('pos[%d]: %d, stop: %.2f, trailPrc: %.2f' % (
                i, self.targetPos_ar[i], self.stopPrc_ar[i], self.trailPrc_ar[i]))

        msg = '%s : ' % bar.datetime.strftime('%Y-%m-%d %H:%M:%S')
        for i in range(self.n):
            msg += '[%d]=%.1f ' % (i, self.signal_ar[i])
        self.write_log('%s targetpos=%d H:%.2f,L:%.2f,C:%.2f' % (msg, self.targetPos, high[-1], low[-1], close[-1]))
        # self.write_log('ht_trend(close):%.2f close+atr"%.2f\n' % ((ht_trend(close))[-1], (close+atr(high, low, close, 15))[-1]))

    def on_order(self, order):
        if order.status == Status.SUBMITTING.value:
            return

        if order.vt_order_id in self.order_dict.keys():
            if order.direction == Direction.LONG.value:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos += new_traded
                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)
                    if order.status == Status.CANCELLED.value:
                        self.order_aggr += 1
                    elif order.status == Status.ALLTRADED.value:
                        self.order_aggr = 0
            else:
                bef_order = self.order_dict.get(order.vt_order_id)
                new_traded = order.traded - bef_order.traded
                if new_traded > 0:
                    self.pos -= new_traded
                self.order_dict[order.vt_order_id] = copy(order)
                if not order.is_active():
                    self.order_dict.pop(order.vt_order_id)
                    if order.status == Status.CANCELLED.value:
                        self.order_aggr += 1
                    elif order.status == Status.ALLTRADED.value:
                        self.order_aggr = 0

        self.write_log("[on_order info] vt_order_id:{}, order.status:{},info:{},{},{},{},{}".format(
            order.vt_order_id, order.status, order.vt_symbol, order.direction, order.price, order.volume, order.traded))

    def on_trade(self, trade):
        msg = '[write_trade] {},{},{},{},{},{},{}'.format(trade.trade_time, trade.vt_symbol, trade.vt_trade_id,
                                                          trade.direction, trade.offset, trade.price, trade.volume)
        self.write_log(msg)
