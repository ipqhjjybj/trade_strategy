# coding=utf-8


class BaseApp(object):

    app_name = ""           # Unique name used for creating engine and widget
    app_module = ""         # App module string used in import_module
    app_path = ""           # Absolute path of app folder
    display_name = ""       # Name for display on the menu.
    engine_class = None     # App engine class
    widget_name = ""        # Class name of app widget

