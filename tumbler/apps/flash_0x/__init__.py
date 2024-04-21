# coding=utf-8

from pathlib import Path
from tumbler.app import BaseApp

from .base import APP_NAME
from .conf_make_flash import parse_flash_make_work_dir
from .conf_make_super import parse_super_make_work_dir
from .engine import Flash0xEngine


class Flash0xApp(BaseApp):
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "做市策略"
    engine_class = Flash0xEngine
    widget_name = "Maker0xApp"

