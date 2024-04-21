# coding=utf-8

from pathlib import Path

from tumbler.app import BaseApp

from .base import APP_NAME
from .engine import CtaEngine


class CtaApp(BaseApp):

    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "CTA_Strategy"
    engine_class = CtaEngine
    widget_name = "CtaApp"
