"""
General utility functions.
"""

import json
from pathlib import Path
import os
import codecs


def _get_trader_dir(temp_name):
    """
    Get path where trader is running in.
    """
    cwd = Path.cwd()
    temp_path = cwd.joinpath(temp_name)

    # If .tumbler folder exists in current working directory,
    # then use it as trader running path.
    if temp_path.exists():
        return cwd, temp_path

    # Otherwise use home path of system.
    # home_path = os.environ.get("HOME",".")
    home_path = "."
    temp_path = os.path.join(home_path, temp_name)

    # Create .tumbler folder under home path if not exist.
    if not os.path.exists(temp_path):
        os.mkdir(temp_path)

    return home_path, temp_path


TRADER_DIR, TEMP_DIR = _get_trader_dir(".tumbler")


def get_file_path(filename):
    """
    Get path for temp file with filename.
    """
    return os.path.join(str(TEMP_DIR), str(filename))


def get_folder_path(folder_name):
    """
    Get path for temp folder with folder name.
    """
    folder_path = os.path.join(str(TEMP_DIR), str(folder_name))
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)
    return folder_path


def get_icon_path(file_path, ico_name):
    """
    Get path for icon file with ico name.
    """
    ui_path = Path(file_path).parent
    icon_path = os.path.join(str(ui_path), "ico", ico_name)

    return str(icon_path)


def load_json(filename):
    """
    Load data from json file in temp path.
    """
    if os.path.exists(filename):
        with codecs.open(filename, mode="r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    else:
        save_json(filename, {})
        return {}


def simple_load_json(filename):
    """
        Load data from json file in temp path.
        """
    if os.path.exists(filename):
        with codecs.open(filename, mode="r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    else:
        return {}


def save_json(filename, data):
    """
    Save data into json file in temp path.
    """
    with codecs.open(filename, mode="w+", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
