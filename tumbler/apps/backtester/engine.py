# coding=utf-8

from tumbler.apps.data_third_part.base import (
    APP_NAME
)
from tumbler.engine import BaseEngine


class BacktesterEngine(BaseEngine):
    """
    For running strategy backtesting!
    """

    def __init__(self, main_engine, event_engine):
        super(BacktesterEngine, self).__init__(main_engine, event_engine, APP_NAME)
