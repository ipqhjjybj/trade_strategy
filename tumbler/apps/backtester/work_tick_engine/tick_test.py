import sys
import numpy as np
# from qpython import *
import heapq

from tumbler.apps.backtester.new_tick_engine import *

from .test_strategy import TestStrategy

'''
用trade算 bidfill与askfill
amt 是成交金额, pa是prev_ask, pb是prev_bid

pa:a^prev a;
pb:b^prev b;
askfill:(amt-v*pb)%(pa-pb);
askfill:?[askfill>v;v;?[askfill<0;0;askfill]];
bidFill:vol-askFill;
afPrice:prev ask1;
bfPrice:prev bid1

[公式来源]
askFillPrice * askFill + bidFillPrice * bidFill = amount
askFill + bidFill = volume

afPrice 近似于 prev_ask_price
bfPrice 近似于 prev_bid_price

ContractType: pandas
index:                   手续费计数
symType(spot), btc_usdt, value, 0 , commType, commValue, factor
symType(spot), eth_usdt, value, 0 , commType, commValue, factor

tick: dict
        date,    2019-01-01 10:10:10 .5
        ask1,    3943
        askvol1,  300  
        bid1,      3942.5
        bidvol1,    50
        bidFill,        推算出来的 bidFill 数量
        askFill,        推算出来的 askFill 数量
        afPrice,        askFill 的价格, 
        bfPrice,        bidFill 的价格

tbl: dict
    symType:
    commType:
    commValue:
    factor:


'''


def parseTicks():
    '''

    '''
    pass


def test():
    t_strategy = TestStrategy(recordFloatPnL=True)
    tick_engine = TickEngine(fillParam=1.0, queueParam=1.0, debug=False, logfileName='', qip='127.0.0.1', qport=5001)

    ticks = []
    tick_engine.RegisterStrategy(t_strategy)
    tick_engine.Start(ticks)


if __name__ == "__main__":
    test()
