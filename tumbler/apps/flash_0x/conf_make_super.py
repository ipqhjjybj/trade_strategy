# coding=utf-8

import os

from tumbler.constant import Exchange
from tumbler.function import TRADER_DIR, write_file, save_json
from tumbler.encryption import my_decrypt

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


template_run_file = '''
# coding=utf-8

from time import sleep

from tumbler.constant import Exchange
from tumbler.event import EventEngine
from tumbler.engine import MainEngine
from tumbler.gateway.huobi import HuobiGateway
from tumbler.gateway.gateio import GateioGateway
from tumbler.gateway.binance import BinanceGateway
from tumbler.gateway.okex import OkexGateway
from tumbler.gateway.bitfinex import BitfinexGateway
from tumbler.gateway.bittrex import BittrexGateway
from tumbler.gateway.coinex import CoinexGateway
from tumbler.gateway.mov import MovGateway
from tumbler.gateway.movflash import FlashGateway
from tumbler.gateway.movsuper import SuperGateway
from tumbler.apps.data_third_part import DataThirdPartApp
from tumbler.apps.monitor import MonitorApp
from tumbler.apps.flash_0x import Flash0xApp
from tumbler.object import SubscribeRequest
from tumbler.function import get_from_vt_key
from tumbler.function import parse_get_super_settings, parse_get_monitor_setting, load_json
from tumbler.service.log_service import log_service_manager

gateway_dict = {
    Exchange.MOV.value: MovGateway,
    Exchange.HUOBI.value: HuobiGateway,
    Exchange.GATEIO.value: GateioGateway,
    Exchange.BINANCE.value: BinanceGateway,
    Exchange.OKEX.value: OkexGateway,
    Exchange.BITFINEX.value: BitfinexGateway,
    Exchange.BITTREX.value: BittrexGateway,
    Exchange.COINEX.value: CoinexGateway,
    Exchange.FLASH.value: FlashGateway,
    Exchange.SUPER.value: SuperGateway
}


def run_child():
    connect_setting = load_json("connect_setting.json")
    setting = load_json("flash_maker_setting.json")
    monitor_settings = parse_get_monitor_setting(setting)
    vt_symbols = parse_get_super_settings(setting)

    event_engine = EventEngine()
    event_engine.start()
    main_engine = MainEngine(event_engine)

    all_exchanges = connect_setting.keys()
    for exchange in all_exchanges:
        if exchange in gateway_dict:
            u_gate = gateway_dict[exchange]
            main_engine.add_gateway(u_gate)
            log_service_manager.write_log("main_engine.add_gateway({})".format(exchange))

    data_third_part = main_engine.add_app(DataThirdPartApp)
    flash_maker = main_engine.add_app(Flash0xApp)
    monitor_app = main_engine.add_app(MonitorApp)

    monitor_app.load_setting(monitor_settings)

    main_engine.write_log("create engine success!")

    log_engine = main_engine.get_engine("log")

    for exchange in all_exchanges:
        c_setting = connect_setting[exchange]
        main_engine.connect(c_setting, exchange)

    main_engine.write_log("connect gateway success!")

    sleep(5)

    for vt_symbol in vt_symbols:
        symbol, exchange = get_from_vt_key(vt_symbol)
        sub = SubscribeRequest()
        sub.symbol = symbol
        sub.exchange = exchange
        sub.vt_symbol = vt_symbol

        log_service_manager.write_log("subscribe:{},{}".format(symbol, exchange))
        main_engine.subscribe(sub, exchange)

    flash_maker.init_engine()
    flash_maker.init_all_strategies()
    main_engine.write_log("flash_maker init finished!")

    sleep(5)

    flash_maker.start_all_strategies()

    log_service_manager.write_log("flash_maker.start_all_strategies()")

    input()


if __name__ == "__main__":
    run_child()

'''


def parse_super_make_work_dir(strategy_setting, connect_setting):
    for strategy_name, dic in strategy_setting.items():
        strategy_run_dir = os.path.join(TRADER_DIR, ".strategy_run")

        if not os.path.exists(strategy_run_dir):
            os.mkdir(strategy_run_dir)

        dir_path = os.path.join(strategy_run_dir, strategy_name)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        setting_content_json = {strategy_name: dic}
        setting_file_path = os.path.join(dir_path, "flash_maker_setting.json")

        # 导出setting 配置
        save_json(setting_file_path, setting_content_json)

        run_file_path = os.path.join(dir_path, "{}.py".format(strategy_name))
        write_file(run_file_path, template_run_file)

        shell_path = os.path.join(dir_path, "restart.sh")
        write_file(shell_path, get_restart_shell_content(strategy_name))

        all_exchanges = set([])
        all_exchanges.add(dic["setting"]["target_exchange_info"]["exchange_name"])
        if "base_exchange_info" in dic["setting"].keys():
            all_exchanges.add(dic["setting"]["base_exchange_info"]["exchange_name"])

        connect_setting_json = {}
        for exchange in all_exchanges:
            connect_setting_json[exchange] = connect_setting[exchange]
            if exchange == Exchange.SUPER.value:
                connect_setting_json[exchange]["api_key"] = dic["setting"]["target_exchange_info"]["guid"]
                connect_setting_json[exchange]["secret_key"] = my_decrypt(
                    dic["setting"]["target_exchange_info"]["secret_key"])

        connect_file_path = os.path.join(dir_path, "connect_setting.json")
        save_json(connect_file_path, connect_setting_json)
