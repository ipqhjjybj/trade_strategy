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
from tumbler.gateway.movflash import FlashGateway
from tumbler.apps.market_maker import MarketMakerApp
from tumbler.apps.data_third_part import DataThirdPartApp
from tumbler.apps.monitor import MonitorApp
from tumbler.event import EVENT_LOG, EVENT_TRADE, Event
from tumbler.object import SubscribeRequest, TradeData
from tumbler.function import parse_get_data_third_part_setting, parse_get_monitor_setting, load_json
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
    Exchange.FLASH.value: FlashGateway
}


def run_child():
    connect_setting = load_json("connect_setting.json")
    setting = load_json("flash_maker_setting.json")

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
        for symbol, _ in data_third_part_settings["merge_ticks"]:
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
from tumbler.gateway.movflash import FlashGateway
from tumbler.apps.data_third_part import DataThirdPartApp
from tumbler.apps.monitor import MonitorApp
from tumbler.apps.flash_0x import Flash0xApp
from tumbler.object import SubscribeRequest, TradeData
from tumbler.function import parse_get_data_third_part_setting, parse_get_monitor_setting, load_json
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
}


def run_child():
    connect_setting = load_json("connect_setting.json")
    setting = load_json("flash_maker_setting.json")
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

    for exchange in all_exchanges:
        for symbol, _ in data_third_part_settings["merge_ticks"]:
            log_service_manager.write_log("subscribe:{},{}".format(symbol, exchange))

            sub = SubscribeRequest()
            sub.symbol = symbol
            sub.exchange = exchange
            sub.vt_symbol = "{}.{}".format(symbol, exchange)

            main_engine.subscribe(sub, exchange)

    data_third_part.init_engine()
    data_third_part.load_from_setting(setting=data_third_part_settings)

    main_engine.write_log("data_third_part init finished!")

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

shell_run_deal_file = "nohup ../dealer ./{}.conf > run_{}.log &"
shell_run_deal_file_v3 = "nohup ../../dealer_v3 ./{}.conf > run_{}.log &"

'''
old:nohup /home/admin/src/tumbler/run_tumbler/tools/linux_dealer ./${symbol_conf_file} > run.log 2>&1 &
now:
'''

restart_dealer_v3 = '''#!/bin/bash
symbol_conf_file="%s.conf"
'''
restart_append_msg = '''
if [ $(ps -ef | grep "${symbol_conf_file}" | grep -v "grep" |  wc -l) -ge 1 ];then
    echo "restart run ${symbol_conf_file}"
    kill $(ps -ef|grep "${symbol_conf_file}"| grep -v "grep"|awk '{print $2}')
    echo "has kill ${symbol_conf_file}"
    sleep 1

    time=$(date "+%Y%m%d%H%M%S")
    zip -r ${time}_bak.zip .tumbler *.log logs
    rm -rf .tumbler logs

    nohup /home/admin/src/tumbler/run_tumbler/tools/linux_dealer_hz_test ./${symbol_conf_file} > run.log 2>&1 &

elif [ $(ps -ef | grep "${symbol_conf_file}"| grep -v "grep" |  wc -l) -eq 0 ];then
    echo "just start run ${symbol_conf_file}"

    time=$(date "+%Y%m%d%H%M%S")
    zip -r ${time}_bak.zip .tumbler *.log logs
    rm -rf .tumbler logs

    nohup /home/admin/src/tumbler/run_tumbler/tools/linux_dealer_hz_test ./${symbol_conf_file} > run.log 2>&1 &
else
    echo "terrible error"
fi
'''


def get_restart_dealer_content(conf_name):
    data = restart_dealer_v3 % (conf_name)
    data = data + restart_append_msg
    return data


def parse_flash_make_work_dir(strategy_setting, connect_setting):
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
        setting_file_path = os.path.join(dir_path, "flash_maker_setting.json")

        # 导出setting 配置
        save_json(setting_file_path, setting_content_json)

        cancel_order_file_path = os.path.join(dir_path, "cancel_order.py")
        write_file(cancel_order_file_path, template_cancel_file)

        run_file_path = os.path.join(dir_path, "{}.py".format(strategy_name))
        write_file(run_file_path, template_run_file)

        shell_path = os.path.join(dir_path, "restart.sh")
        write_file(shell_path, get_restart_shell_content(strategy_name))

        all_exchanges = set([])
        all_exchanges.add(dic["setting"]["target_exchange_info"]["exchange_name"])

        for exchange in dic["setting"]["base_exchange_info"].keys():
            all_exchanges.add(exchange)

        deals_name = dic.get("deals_name", "")
        deals_arr = dic.get("deals", [])

        address = ""
        before_xprv = ""
        for deals in deals_arr:
            if 'v3' in deals["flash_swap_url"] or "50052" in deals["flash_swap_url"]:
                deals_run_dir = os.path.join(TRADER_DIR, ".deal_v3")
                need_shell_file = shell_run_deal_file_v3
                deals_dir = os.path.join(deals_run_dir, deals_name)
            else:
                deals_run_dir = os.path.join(TRADER_DIR, ".deal")
                need_shell_file = shell_run_deal_file
                deals_dir = deals_run_dir
            if not os.path.exists(deals_run_dir):
                os.mkdir(deals_run_dir)

            if not os.path.exists(deals_dir):
                os.mkdir(deals_dir)
            deals_file_name = "{}.conf".format(deals_name)
            deals_path = os.path.join(deals_dir, deals_file_name)

            restart_file_content = get_restart_dealer_content(deals_name)
            restart_file_name = "restart.sh".format(deals_name)
            restart_path = os.path.join(deals_dir, restart_file_name)
            write_file(restart_path, restart_file_content)

            if "xprv" in deals.keys():
                before_xprv = deals["xprv"]
                deals["xprv"] = my_decrypt(deals["xprv"])
            save_json(deals_path, deals)

            shell_deal_file_name = "run_{}.sh".format(deals_name)
            shell_deal_path = os.path.join(deals_dir, shell_deal_file_name)
            data = need_shell_file.format(deals_name, deals_name)
            write_file(shell_deal_path, data)

            if "address" in deals.keys():
                address = deals["address"]

        port_arr = [deals["port"] for deals in deals_arr]

        connect_setting_json = {}
        for exchange in all_exchanges:
            connect_setting_json[exchange] = connect_setting[exchange]
            if exchange == Exchange.FLASH.value:
                connect_setting_json[exchange]["api_key"] = address
                connect_setting_json[exchange]["secret_key"] = before_xprv

                urls = ["http://127.0.0.1:{}".format(port) for port in port_arr]
                connect_setting_json[exchange]["local_url"] = urls

        connect_file_path = os.path.join(dir_path, "connect_setting.json")
        save_json(connect_file_path, connect_setting_json)
