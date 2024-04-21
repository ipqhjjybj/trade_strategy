# coding=utf-8

from pathlib import Path

from tumbler.app import BaseApp

from .base import APP_NAME
from .engine import FutureSpotSpreadEngine


class FutureSpotSpreadApp(BaseApp):

    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "期现套利"
    engine_class = FutureSpotSpreadEngine
    widget_name = "FutureSpotSpreadManager"

