# coding=utf-8

import os
from copy import copy

from tumbler.function import load_json, write_file, save_json
from tumbler.service import log_service_manager

aggr_tpl_msg = '''# coding=utf-8

import time
from tumbler.engine import MainEngine
from tumbler.aggregation import Aggregation

from tumbler.event import EventEngine
from tumbler.event import (
    EVENT_MERGE_TICK,
    EVENT_TICK
)


def print_tick(e):
    print("tick:", e.data.__dict__)


def print_merge_tick(e):
    print("merge:", e.data.__dict__)


def print_log(e):
    print(e.data.msg)


"""
这是程序 实际 运行调用的例子
"""


def run():
    e = EventEngine()
    e.start()

    #e.register(EVENT_MERGE_TICK, print_merge_tick)
    #e.register(EVENT_TICK, print_tick)

    a = Aggregation(e, "aggregation_setting.json")

    a.connect()

    input()


if __name__ == "__main__":
    run()

'''

restart_shell_file = '''#!/bin/bash
if [ $(ps -ef | grep "%s.py"| grep -v "grep" |  wc -l) -ge 1 ];then
    echo "restart %s.py "
    kill $(ps -ef|grep "%s.py"| grep -v "grep"|awk '{print $2}')
    echo "has kill %s.py"
    sleep 1

    time=$(date "+%%Y%%m%%d%%H%%M%%S")
    zip -r ${time}_bak.zip .tumbler *.log
    rm -f *.log
    rm -rf .tumbler

    nohup python3 %s.py > run.log 2>&1 &

elif [ $(ps -ef | grep "%s.py"| grep -v "grep" |  wc -l) -eq 0 ];then
    echo "just start python %s.py "

    time=$(date "+%%Y%%m%%d%%H%%M%%S")
    zip -r ${time}_bak.zip .tumbler *.log
    rm -f *.log
    rm -rf .tumbler

    nohup python3 %s.py > run.log 2>&1 &
else
    echo "terrible error"
fi
'''


def get_restart_shell_content(strategy_name):
    data = restart_shell_file % (
        strategy_name, strategy_name, strategy_name, strategy_name, strategy_name, strategy_name, strategy_name,
        strategy_name)
    return data


def aggr_conf_make():
    parent_dir = "all_ws_latest"
    if not os.path.exists(parent_dir):
        os.mkdir(parent_dir)
    aggr_dic = load_json("aggregation_setting.json")

    for symbol_key, exchanges in aggr_dic["symbols"].items():
        log_service_manager.write_log(f"symbol_key:{symbol_key} exchanges:{exchanges}")

        work_dir = os.path.join(parent_dir, symbol_key)

        if not os.path.exists(work_dir):
            log_service_manager.write_log(f"[log] create work_dir:{work_dir}")
            os.mkdir(work_dir)

        ws_file = f"ws_aggr_{symbol_key}.py"
        ws_path = os.path.join(work_dir, ws_file)
        log_service_manager.write_log(f"[log] create ws_path:{ws_path}")
        write_file(ws_path, aggr_tpl_msg)

        copy_aggr_dic = copy(aggr_dic)
        copy_aggr_dic["symbols"] = {}
        copy_aggr_dic["symbols"][symbol_key] = exchanges

        aggr_json_file = os.path.join(work_dir, "aggregation_setting.json")
        log_service_manager.write_log(f"[log] create aggr_json_file:{aggr_json_file}")
        save_json(aggr_json_file, copy_aggr_dic)

        shell_file = os.path.join(work_dir, "restart.sh")
        restart_shell_content = get_restart_shell_content(ws_file.split(".")[0])
        write_file(shell_file, restart_shell_content)
