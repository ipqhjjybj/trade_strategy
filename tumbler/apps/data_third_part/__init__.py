# coding=utf-8

from pathlib import Path

from tumbler.app import BaseApp

from .base import APP_NAME
from .engine import DataThirdPartEngine


class DataThirdPartApp(BaseApp):

    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "其他数据源"
    engine_class = DataThirdPartEngine
    widget_name = "DataThirdPartManager"
