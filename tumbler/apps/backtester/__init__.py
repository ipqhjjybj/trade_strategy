# coding=utf-8

from pathlib import Path

from tumbler.app import BaseApp

from .base import APP_NAME
from .engine import BacktesterEngine


class BacktesterApp(BaseApp):

    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "回测模块"
    engine_class = BacktesterEngine
    widget_name = "BacktesterManager"
