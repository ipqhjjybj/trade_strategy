# coding=utf-8

from pathlib import Path

from tumbler.app import BaseApp

from .base import APP_NAME
from .engine import CalendarSpreadEngine


class CalendarSpreadApp(BaseApp):

    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "跨期套利"
    engine_class = CalendarSpreadEngine
    widget_name = "CalendarSpreadManager"

