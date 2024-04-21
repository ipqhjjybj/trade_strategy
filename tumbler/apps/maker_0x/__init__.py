# coding=utf-8

from pathlib import Path
from tumbler.app import BaseApp

from .base import APP_NAME
from .engine import Maker0xEngine


class Maker0xApp(BaseApp):

    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "做市策略"
    engine_class = Maker0xEngine
    widget_name = "Maker0xApp"

