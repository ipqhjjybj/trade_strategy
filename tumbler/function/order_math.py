# coding=utf-8

from decimal import Decimal
from tumbler.constant import Exchange


def get_round_order_price(price, price_tick):
    """
    根据交易所的每个合约的最小交易精度数据，得到准备的下单价格
    up=True 是 返回价格 >= price  (通过加一个或0个精度得到)
    up=False 是 返回价格 <= price (通过减1一个或0个精度得到)
    """
    if price_tick < 1e-12:
        return price
    price = Decimal(str(price))
    price_tick = Decimal(str(price_tick))
    rounded = float(int(round(price / price_tick)) * price_tick)
    return rounded


def get_volume_tick_from_min_volume(min_volume):
    """
    有交易所 有最小下单量， 没有下单数量的精度，那么就内部规定如
    0.005 --> 最小下单量 0.001
    0.05 --> 最小下单量  0.01
    10 ---> 最小下单量 1
    5  --> 最小下单量 1
    """
    min_volume = str(min_volume)
    if '.' in min_volume:
        return 10 ** (-1 * len(min_volume.split('.')[-1]))
    else:
        return 1


def is_number_change(p1, p2):
    if abs(p2 - p1) > 0.001 * max(abs(p1), abs(p2)):
        return True
    else:
        return False


system_inside_min_volume = {
    "_usdt": 0.0006,
    "_btc": 0.0006,
    "_eth": 0.014
}

binance_inside_min_volume = {
    "_usdt": 10
}


def get_system_inside_min_volume(symbol, price, exchange):
    if price < 1e-12:
        return 0.0

    dic = system_inside_min_volume

    if exchange == Exchange.BINANCE.value:
        dic = binance_inside_min_volume

    for key, val in dic.items():
        if symbol.endswith(key):
            return float(val) / price

    return 0.0


def is_price_volume_too_small(symbol, price, volume):
    for key, val in system_inside_min_volume.items():
        if symbol.endswith(key):
            if price * volume < val:
                return True
    return False


def my_str(x, precision=10):
    if isinstance(x, float):
        return "%.{}f".format(precision) % x
    else:
        return str(x)
