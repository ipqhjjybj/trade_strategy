# coding=utf-8

from pathlib import Path

from tumbler.app import BaseApp

from .base import APP_NAME
from .engine import MonitorEngine


class MonitorApp(BaseApp):
    """
    监控引擎
    包含 统计成交记录，列成表格，定时统计净值等
    """
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "监控插件"
    engine_class = MonitorEngine
    widget_name = "Monitor"

