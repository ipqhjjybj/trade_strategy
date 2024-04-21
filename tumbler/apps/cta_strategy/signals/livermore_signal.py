# coding=utf-8

import time
from datetime import datetime
from copy import copy
from collections import defaultdict
from threading import Lock
from enum import Enum

from tumbler.apps.cta_strategy.template import (
    CtaTemplate,
    CtaSignal
)
from tumbler.object import OrderData
from tumbler.constant import MAX_PRICE_NUM, Exchange, MIN_FLOAT_VAL
from tumbler.function import get_vt_key, get_round_order_price, get_two_currency
from tumbler.constant import Direction, Status, Offset, OrderType, StopOrderStatus, Interval
from tumbler.function.bar import BarGenerator, ArrayManager
from tumbler.function.technique import Technique


class LivermoreCons(Enum):
    """
    Direction of LivermoreCons
    """
    ShangShenQuShi = "ssqs"                 # 上升趋势
    ZiRanHuiShen = "zrhs"                   # 自然回升
    CiJiHuiShen = "cjhs"                    # 次级回升

    XiaJiangQushi = "xjqs"                  # 下降趋势
    ZiRanHuiChe = "zrhc"                    # 自然回撤
    CiJiHuiChe = "cjhc"                     # 次级回撤

    HeiMoShui = "b"                         # 黑墨水
    HongMoShui = "r"                        # 红墨水
    QianBi = "w"                            # 铅笔

    NoBelowLine = "no"                      # 没有下划线
    RED_LINE = "red"                        # 上升趋势以及自然回调后形成的点标记
    BLACK_LINE = "black"                    # 下降趋势以及自然回升形成的点下标记


class LivermorePoint(object):
    """
    """
    def __init__(self, _datetime, _y, _color):
        self.datetime = _datetime
        self.y = _y
        self.color = _color


class LivermoreLine(object):
    """
    """
    def __init__(self, _datetime, _y, _bigcond, _color):
        self.datetime = _datetime
        self.y = _y
        self.bidcond = _bigcond
        self.color = _color


class LivermoreSignal(CtaSignal):


    def __init__(self, _param1, _param2):
        self.param1 = _param1
        self.param2 = _param2

        self.key_point_arr = []             # 存储最重要的几个关键点，  (点位,时间, 线的颜色)的格式
        self.line_point_arr = []            # [(datetime,Y,"上升趋势",'r')] 存储剩下的趋势点, 上升趋势黑墨水 k--black，下降趋势红墨水 ， 其他栏的点，铅笔
        self.number_ssqs = []               # 上升趋势
        self.number_zrhs = []               # 自然回升
        self.number_cjhs = []               # 次级回升
        self.number_xjqs = []               # 下降趋势
        self.number_zrhc = []               # 自然回撤
        self.number_cjhc = []               # 次级回撤

        self.big_cond_arr = []              # 大方向状态数组
        self.big_condition = LivermoreCons.ShangShenQuShi.value    # 大方向状态 
        self.conditionChangeType = 0        # 状态变更的原因

    def add_to_number_figure(self, _datetime, y, _big_condition):
        color = LivermoreCons.QianBi.value
        if _big_condition == LivermoreCons.ShangShenQuShi.value:
            self.number_ssqs.append( LivermorePoint(_datetime, y, LivermoreCons.NoBelowLine.value))
            color = LivermoreCons.HeiMoShui.value

        if _big_condition == LivermoreCons.ZiRanHuiShen.value:
            self.number_zrhs.append( LivermorePoint(_datetime, y, LivermoreCons.NoBelowLine.value))

        if _big_condition == LivermoreCons.CiJiHuiShen.value:
            self.number_cjhs.append( LivermorePoint(_datetime, y, LivermoreCons.NoBelowLine.value))

        if _big_condition == LivermoreCons.XiaJiangQushi.value:
            self.number_xjqs.append( LivermorePoint(_datetime, y, LivermoreCons.NoBelowLine.value))

        if _big_condition == LivermoreCons.ZiRanHuiChe.value:
            self.number_zrhc.append( LivermorePoint(_datetime, y, LivermoreCons.NoBelowLine.value))

        if _big_condition == LivermoreCons.CiJiHuiChe.value:
            self.number_cjhc.append( LivermorePoint(_datetime, y, LivermoreCons.NoBelowLine.value))

        self.key_point_arr.append(LivermoreLine(_datetime, y, _big_condition, color))

    def judge_xjqs(self, _big_condition, y):
        p = _big_condition
        if len(self.number_xjqs) > 0:
            if y < self.number_xjqs[-1].y:
                self.conditionChangeType = 3

        to_drop_line = 0
        if len(self.number_zrhc) > 0:
            for i in range(1, len(self.number_zrhc) + 1):
                if self.number_zrhc[-i].color == LivermoreCons.RED_LINE.value:
                    if y < self.number_zrhc[-i].y * (1 - self.param2):
                        self.conditionChangeType = 4
                        p = LivermoreCons.XiaJiangQushi.value
                        to_drop_line = 1

                    break
        if 1 == to_drop_line:
            for i in range(1, len(self.number_zrhc)+1):
                if self.number_zrhc[-i].color == LivermoreCons.RED_LINE.value:
                    self.number_zrhc[-i].color = LivermoreCons.NoBelowLine.value

        return p
    
    def judge_ssqs(self, _big_condition, y):
        p = _big_condition
        if len(self.number_ssqs) > 0:
            if y < self.number_ssqs[-1].y:
                self.conditionChangeType = 1
                p = LivermoreCons.ShangShenQuShi.value

        to_drop_line = 0
        if len(self.number_zrhs) > 0:
            for i in range(1, len(self.number_zrhs) + 1):
                if self.number_zrhs[-i].color == LivermoreCons.BLACK_LINE.value:
                    if y > self.number_zrhs[-i].y * (1 + self.param2):
                        self.conditionChangeType = 2
                        p = LivermoreCons.ShangShenQuShi.value
                        to_drop_line = 1

        if 1 == to_drop_line:
            for i in range(1, len(self.number_zrhs) + 1):
                if self.number_zrhs[-i].color == LivermoreCons.BLACK_LINE.value:
                    self.number_zrhs[-i].color = LivermoreCons.NoBelowLine.value

        return p

    def judge(self, t_kline_point, x, y):
        pl_x = t_kline_point.datetime
        pl_y = t_kline_point.y
        pl_condition = t_kline_point.bidcond
        pl_color = t_kline_point.color

        if self.big_condition == LivermoreCons.ShangShenQuShi.value:
            if y > pl_y:
                self.add_to_number_figure(x, y, self.big_condition)
            elif y < pl_y * (1 - self.param1):
                self.key_point_arr.append(LivermorePoint(pl_x, pl_y, LivermoreCons.RED_LINE.value))
                self.number_ssqs[-1].color = LivermoreCons.RED_LINE.value
                self.big_condition = LivermoreCons.ZiRanHuiChe.value
                self.add_to_number_figure(x, y, self.big_condition)
        elif self.big_condition == LivermoreCons.ZiRanHuiShen.value:
            if y > pl_y:
                self.big_condition = self.judge_ssqs(self.big_condition, y)
                self.add_to_number_figure(x, y, self.big_condition)
            elif y < pl_y * (1 - self.param1):
                self.key_point_arr.append(LivermorePoint(pl_x, pl_y, LivermoreCons.BLACK_LINE.value))
                self.number_zrhs[-1].color = LivermoreCons.BLACK_LINE.value
                self.big_condition = LivermoreCons.ZiRanHuiChe.value

                if len(self.number_zrhc) > 0 and y > self.number_zrhc[-1].y:
                    self.big_condition = LivermoreCons.CiJiHuiChe.value
                if len(self.number_zrhc) == 0:
                    self.big_condition = LivermoreCons.ZiRanHuiShen.value

                self.big_condition = self.judge_xjqs(self.big_condition, y)
                self.add_to_number_figure(x, y, self.big_condition)

        elif self.big_condition == LivermoreCons.CiJiHuiShen.value:
            if y > pl_y:
                if len(self.number_zrhs) > 0 and y > self.number_zrhs[-1].y:
                    self.big_condition = LivermoreCons.ZiRanHuiShen.value
                if len(self.number_zrhs) == 0:
                    self.big_condition = LivermoreCons.ZiRanHuiShen.value

                self.big_condition = self.judge_ssqs(self.big_condition, y)
                self.add_to_number_figure(x, y, self.big_condition)
            elif y < pl_y * (1 - self.param1):
                self.big_condition = LivermoreCons.CiJiHuiChe.value
                if len(self.number_zrhc) > 0 and y < self.number_zrhc[-1].y:
                    self.big_condition = LivermoreCons.ZiRanHuiChe.value
                if len(self.number_zrhc) == 0:
                    self.big_condition = LivermoreCons.ZiRanHuiChe.value
                
                self.big_condition = self.judge_xjqs(self.big_condition, y)
                self.add_to_number_figure(x, y, self.big_condition)

        elif self.big_condition == LivermoreCons.XiaJiangQushi.value:
            if y < pl_y:
                self.add_to_number_figure(x, y, self.big_condition)
            elif y > pl_y * (1 + self.param1):
                self.key_point_arr.append(LivermorePoint(pl_x, pl_y, LivermoreCons.BLACK_LINE.value))
                self.number_xjqs[-1].color = LivermoreCons.BLACK_LINE.value
                self.big_condition = LivermoreCons.ZiRanHuiShen.value
                self.add_to_number_figure(x, y, self.big_condition)

        elif self.big_condition == LivermoreCons.ZiRanHuiChe.value:
            if y < pl_y:
                self.big_condition = self.judge_xjqs(self.big_condition, y)
                self.add_to_number_figure(x, y, self.big_condition)
            elif y > pl_y * (1 + self.param1):
                self.key_point_arr.append(LivermorePoint(pl_x, pl_y, LivermoreCons.RED_LINE.value))
                self.number_zrhc[-1].color = LivermoreCons.RED_LINE.value
                self.big_condition = LivermoreCons.ZiRanHuiShen.value
                # 判断是否是次级回升
                if len(self.number_zrhs) > 0 and y < self.number_zrhs[-1].y:
                    self.big_condition = LivermoreCons.CiJiHuiShen.value
                if len(self.number_zrhs) == 0:
                    self.big_condition = LivermoreCons.ZiRanHuiShen.value
                self.big_condition = self.judge_ssqs(self.big_condition, y)
                self.add_to_number_figure(x, y, self.big_condition)
        elif self.big_condition == LivermoreCons.CiJiHuiChe.value:
            if y < pl_y:
                if len(self.number_zrhc) > 0 and y < self.number_zrhc[-1].y:
                    self.big_condition = LivermoreCons.ZiRanHuiChe.value
                if len(self.number_zrhc) == 0:
                    self.big_condition = LivermoreCons.ZiRanHuiChe.value

                self.big_condition = self.judge_xjqs(self.big_condition, y)
                self.add_to_number_figure(x, y, self.big_condition)
            elif y > pl_y * (1 + self.param1):
                self.big_condition = LivermoreCons.CiJiHuiShen.value
                if len(self.number_zrhs) > 0 and y > self.number_zrhs[-1].y:
                    self.big_condition = LivermoreCons.ZiRanHuiShen.value
                if len(self.number_zrhs) == 0:
                    self.big_condition = LivermoreCons.ZiRanHuiShen.value
                self.big_condition = self.judge_ssqs(self.big_condition, y)
                self.add_to_number_figure(x, y, self.big_condition)

    def on_bar(self, bar):
        if len(self.key_point_arr) == 0:
            self.big_condition = LivermoreCons.ShangShenQuShi.value
            self.add_to_number_figure(bar.datetime, bar.close_price, self.big_condition)
        else:
            self.judge(self.key_point_arr[-1], bar.datetime, bar.close_price)

        self.big_cond_arr.append(self.big_condition)

        pos = 0
        if self.big_condition == LivermoreCons.ShangShenQuShi.value:
            pos = 1 
        elif self.big_condition == LivermoreCons.XiaJiangQushi.value:
            pos = -1 

        self.set_signal_pos(pos)


