# coding=utf-8

from pathlib import Path

from tumbler.app import BaseApp

from .base import APP_NAME
from .engine import RecorderEngine


class DataRecorderApp(BaseApp):
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "Market Data Record"
    engine_class = RecorderEngine
    widget_name = "Market Data Recorder"
