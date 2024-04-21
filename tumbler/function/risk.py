# coding=utf-8

from copy import copy
import time
from datetime import datetime


class TimeWork(object):
    '''
    距离上一次工作时间
    '''
    def __init__(self, sleep_times):
        self.sleep_times = sleep_times
        self.pre_times = time.time() - self.sleep_times

    def can_work(self):
        now = time.time()
        if now >= self.pre_times + self.sleep_times:
            self.pre_times = now
            return True
        return False


class TickDecorder(object):
    '''
    tick 装饰器，
    1、判断tick时间戳是否过期
    2、判断tick是否是旧的时间戳
    '''

    def __init__(self, vt_symbol, strategy):
        self.vt_symbol = vt_symbol
        self.strategy = strategy
        self.tick = None

    def has_not_inited(self):
        return self.tick is None

    def update_tick(self, tick):
        if tick.vt_symbol == self.vt_symbol:
            if self.tick is None:
                self.tick = copy(tick)
            else:
                bef_time = int(time.mktime(self.tick.datetime.timetuple()))
                now_time = int(time.mktime(tick.datetime.timetuple()))
                if now_time >= bef_time:
                    self.tick = copy(tick)
                else:
                    self.strategy.write_important_log("[TickDecorder] [update_tick] error, tick time error! "
                                                      "bef_time:{} now_time:{}".format(bef_time, now_time))

        else:
            self.strategy.write_log("[TickDecorder] [update_tick] update tick error!")

    def is_tick_ok(self, allow_time=10):
        if self.tick is None:
            self.strategy.write_log("[TickDecorder] [is_tick_ok] tick is None!")
            return False
        else:
            bef_time = int(time.mktime(self.tick.datetime.timetuple()))
            now_time = int(time.mktime(datetime.now().timetuple()))

            if now_time - bef_time < allow_time:
                return True
            else:
                self.strategy.write_important_log("[TickDecorder] [is_tick_ok] tick has not updated! "
                                                  "bef_time:{} now_time:{}".format(bef_time, now_time))
                return False

    def get_tick(self):
        return self.tick


class OrderPriceFilter(object):
    '''
    下单价格带保护，离最近的保护价，下单不能偏移太多
    '''

    def __init__(self, vt_symbol, strategy, protect_rate=0.1):
        self.vt_symbol = vt_symbol
        self.strategy = strategy
        self.protect_rate = protect_rate

        self.recent_price = 0

    def update_price(self, price):
        self.recent_price = price

    def is_price_ok(self, order_price):
        if abs(order_price - self.recent_price) < order_price * self.protect_rate:
            return True
        else:
            self.strategy.write_log("[OrderPriceFilter] [is_price_ok] error! vt_symbol:{} order_price:{} "
                                    "recent_price:{}".format(self.vt_symbol, order_price, self.recent_price))
            return False
