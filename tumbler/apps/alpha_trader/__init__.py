# coding=utf-8

'''
这是多因子 交易模块的代码

因为原先的架构，如果跑币种单策略还行，全市场选币下单，就有问题了

因而需要来一个新的架构来做，类似Botvs，直接对市场进行操作

所以得改比较多的地方。。  一个比较大的修改就是 市场行情的选择性订阅，或者从其他地方得到市场行情

'''

from pathlib import Path

from tumbler.app import BaseApp

from .base import APP_NAME
from .engine import AlphaEngine


class AlphaApp(BaseApp):

    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "Alpha_Strategy"
    engine_class = AlphaEngine
    widget_name = "AlphaApp"

