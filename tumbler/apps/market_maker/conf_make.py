# coding=utf-8

import os

from tumbler.function import TRADER_DIR, write_file, save_json
from tumbler.constant import Exchange

restart_shell_file = '''#!/bin/bash
symbol_py_file="%s.py"
cancel_py_file="cancel_%s_order.py"
if [ $(ps -ef | grep "${symbol_py_file}" | grep -v "grep" |  wc -l) -ge 1 ];then
    echo "restart ${symbol_py_file}"
    kill $(ps -ef|grep "${symbol_py_file}"| grep -v "grep"|awk '{print $2}')
    echo "has kill ${symbol_py_file}"

    sleep 1

    nohup python3 ${cancel_py_file} > run_cancel_order.log &
    sleep 15

    if [ $(ps -ef | grep "${cancel_py_file}" | grep -v "grep" |  wc -l) -ge 1 ];then
        echo "run cancel_order.py"
        kill $(ps -ef|grep "${cancel_py_file}"| grep -v "grep"|awk '{print $2}')
    fi

    time=$(date "+%%Y%%m%%d%%H%%M%%S")
    zip -r ${time}_bak.zip .tumbler *.log
    rm -f *.log
    rm -rf .tumbler

    nohup python3 ${symbol_py_file} > run.log 2>&1 &

elif [ $(ps -ef | grep "${symbol_py_file}"| grep -v "grep" |  wc -l) -eq 0 ];then
    echo "just start python ${symbol_py_file}"

    nohup python3 ${cancel_py_file} > run_cancel_order.log &
    sleep 15

    if [ $(ps -ef | grep "${cancel_py_file}" | grep -v "grep" |  wc -l) -ge 1 ];then
        echo "run cancel_order.py"
        kill $(ps -ef|grep "${cancel_py_file}"| grep -v "grep"|awk '{print $2}')
    fi

    time=$(date "+%%Y%%m%%d%%H%%M%%S")
    zip -r ${time}_bak.zip .tumbler *.log
    rm -f *.log
    rm -rf .tumbler

    nohup python3 ${symbol_py_file} > run.log 2>&1 &
else
    echo "terrible error"
fi
'''


def get_restart_shell_content(strategy_name, symbol_pair):
    data = restart_shell_file % (strategy_name, symbol_pair)
    return data


template_cancel_file = '''
# coding=utf-8

from time import sleep

from tumbler.constant import Exchange
from tumbler.event import EventEngine
from tumbler.engine import MainEngine
from tumbler.gateway.huobi import HuobiGateway
from tumbler.gateway.gateio import GateioGateway
from tumbler.gateway.binance import BinanceGateway
from tumbler.gateway.bitfinex import BitfinexGateway
from tumbler.gateway.bittrex import BittrexGateway
from tumbler.gateway.okex import OkexGateway
from tumbler.gateway.mov import MovGateway
from tumbler.gateway.coinex import CoinexGateway
from tumbler.apps.market_maker import MarketMakerApp
from tumbler.apps.data_third_part import DataThirdPartApp
from tumbler.apps.monitor import MonitorApp
from tumbler.event import EVENT_LOG, EVENT_TRADE, Event
from tumbler.object import SubscribeRequest, TradeData
from tumbler.parse import parse_get_data_third_part_setting, parse_get_monitor_setting, load_json
from tumbler.service.log_service import log_service_manager

gateway_dict = {
    Exchange.MOV.value:MovGateway,
    Exchange.HUOBI.value:HuobiGateway,
    Exchange.GATEIO.value:GateioGateway,
    Exchange.BINANCE.value:BinanceGateway,
    Exchange.OKEX.value:OkexGateway,
    Exchange.BITFINEX.value:BitfinexGateway,
    Exchange.BITTREX.value:BittrexGateway,
    Exchange.COINEX.value:CoinexGateway
}


def run_child():
    connect_setting = load_json("connect_setting.json")
    setting = load_json("market_maker_setting.json")
    
    data_third_part_settings = parse_get_data_third_part_setting(setting)
    monitor_settings = parse_get_monitor_setting(setting)

    log_service_manager.write_log(data_third_part_settings)

    event_engine = EventEngine()
    event_engine.start()
    main_engine = MainEngine(event_engine)

    all_exchanges = connect_setting.keys()
    for exchange in all_exchanges:
        if exchange in gateway_dict:
            u_gate = gateway_dict[exchange]
            main_engine.add_gateway(u_gate)
            log_service_manager.write_log("main_engine.add_gateway({})".format(exchange))

    for exchange,c_setting in connect_setting.items():
        main_engine.connect(c_setting, exchange)

    main_engine.write_log("connect gateway successily!")

    sleep(5)

    for exchange in all_exchanges:
        for symbol, _ in data_third_part_settings["ticks"]:
            log_service_manager.write_log("symbol:{},exchange:{}".format(symbol,exchange))
            
            sub = SubscribeRequest()
            sub.symbol = symbol
            sub.exchange = exchange
            sub.vt_symbol = "{}.{}".format(symbol, exchange)

            main_engine.subscribe(sub , exchange)

    input()

if __name__ == "__main__":
    run_child()
'''

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
from tumbler.apps.market_maker import MarketMakerApp
from tumbler.apps.data_third_part import DataThirdPartApp
from tumbler.apps.monitor import MonitorApp
from tumbler.event import EVENT_LOG, EVENT_TRADE, Event
from tumbler.object import SubscribeRequest, TradeData
from tumbler.parse import parse_get_data_third_part_setting, parse_get_monitor_setting, load_json
from tumbler.service.log_service import log_service_manager

gateway_dict = {
    Exchange.MOV.value:MovGateway,
    Exchange.HUOBI.value:HuobiGateway,
    Exchange.GATEIO.value:GateioGateway,
    Exchange.BINANCE.value:BinanceGateway,
    Exchange.OKEX.value:OkexGateway,
    Exchange.BITFINEX.value:BitfinexGateway,
    Exchange.BITTREX.value:BittrexGateway,
    Exchange.COINEX.value:CoinexGateway
}


def run_child():
    connect_setting = load_json("connect_setting.json")
    setting = load_json("market_maker_setting.json")
    data_third_part_settings = parse_get_data_third_part_setting(setting)
    monitor_settings = parse_get_monitor_setting(setting)

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
    market_maker = main_engine.add_app(MarketMakerApp)
    monitor_app = main_engine.add_app(MonitorApp)

    monitor_app.load_setting(monitor_settings)

    main_engine.write_log("create engine successily!")

    log_engine = main_engine.get_engine("log")

    for exchange in all_exchanges:
        c_setting = connect_setting[exchange]
        main_engine.connect(c_setting, exchange)

    main_engine.write_log("connect gateway successily!")

    sleep(5)

    for exchange in all_exchanges:
        for symbol, _ in data_third_part_settings["ticks"]:
            log_service_manager.write_log( "subscribe:{},{}".format(symbol,exchange))
            
            sub = SubscribeRequest()
            sub.symbol = symbol
            sub.exchange = exchange
            sub.vt_symbol = "{}.{}".format(symbol, exchange)

            main_engine.subscribe(sub , exchange)

    data_third_part.init_engine()
    data_third_part.load_from_setting(setting=data_third_part_settings)

    main_engine.write_log("data_third_part init finished!")

    market_maker.init_engine()
    market_maker.init_all_strategies()
    main_engine.write_log("market_maker init finished!")

    sleep(5)

    market_maker.start_all_strategies()

    log_service_manager.write_log("market_maker.start_all_strategies()")

    input()


if __name__ == "__main__":
    run_child()
'''


def parse_make_work_dir(strategy_setting, connect_setting):
    cancel_all_orders_file_path = os.path.join(TRADER_DIR, "cancel_order.py")
    write_file(cancel_all_orders_file_path, template_cancel_file)

    for strategy_name, dic in strategy_setting.items():
        strategy_run_dir = os.path.join(TRADER_DIR, ".strategy_run")

        if not os.path.exists(strategy_run_dir):
            os.mkdir(strategy_run_dir)

        dir_path = os.path.join(strategy_run_dir, strategy_name)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        setting_content_json = {strategy_name: dic}
        setting_file_path = os.path.join(dir_path, "market_maker_setting.json")

        # 导出setting 配置
        save_json(setting_file_path, setting_content_json)

        cancel_order_file_path = os.path.join(dir_path, "cancel_{}_order.py".format(dic["setting"]["symbol_pair"]))
        write_file(cancel_order_file_path, template_cancel_file)

        run_file_path = os.path.join(dir_path, "{}.py".format(strategy_name))
        write_file(run_file_path, template_run_file)

        shell_path = os.path.join(dir_path, "restart.sh")
        write_file(shell_path, get_restart_shell_content(strategy_name, dic["setting"]["symbol_pair"]))

        all_exchanges = set([])
        all_exchanges.add(dic["setting"]["target_exchange_info"]["exchange_name"])

        for exchange in dic["setting"]["base_exchange_info"].keys():
            all_exchanges.add(exchange)

        connect_setting_json = {}
        for exchange in all_exchanges:
            connect_setting_json[exchange] = connect_setting[exchange]
            inside_exchange_config_json = dic.get(exchange, {})
            if inside_exchange_config_json:
                for key in inside_exchange_config_json.keys():
                    connect_setting_json[exchange][key] = inside_exchange_config_json[key]

        connect_file_path = os.path.join(dir_path, "connect_setting.json")
        save_json(connect_file_path, connect_setting_json)
