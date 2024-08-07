# coding=utf-8

from pathlib import Path
from tumbler.app import BaseApp

from .engine import RiskManagerEngine, APP_NAME


class RiskManagerApp(BaseApp):
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "风控模块"
    engine_class = RiskManagerEngine
    widget_name = "RiskManager"

