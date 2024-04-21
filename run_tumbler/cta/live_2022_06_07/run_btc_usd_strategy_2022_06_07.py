# coding=utf-8

import sys
from datetime import datetime, timedelta
from time import sleep

import talib as ta
import numpy as np

from tumbler.constant import Interval
from tumbler.apps.cta_strategy.strategies.factor_multi_period_strategy import FactorMultiPeriodStrategy
from tumbler.constant import Exchange
from tumbler.event import EventEngine
from tumbler.engine import MainEngine
from tumbler.gateway.okex5 import Okex5Gateway
from tumbler.apps.cta_strategy import CtaApp
from tumbler.object import SubscribeRequest
from tumbler.function import load_json

from tumbler.function.technique import PD_Technique
from tumbler.data.binance_data import BinanceClient
from tumbler.service import log_service_manager
from tumbler.constant import EvalType


def display_last(label, arr):
    log_service_manager.write_log(f"[label]:{label}, arr:{arr[len(arr)-1]}!")


def func_hour12(df):
    df = PD_Technique.dmi_strategy(df, length=14, name="pos_1")
    df = PD_Technique.ema_strategy(df, 5, 60, name="pos_2")
    df = PD_Technique.boll_strategy(df, 50, 1, name="pos_3")
    df["pos"] = df["pos_1"] + df["pos_2"] + df["pos_3"]
    display_last("12h dmi_strategy", df["pos_1"])
    display_last("12h ema_strategy", df["pos_2"])
    display_last("12h boll_strategy", df["pos_3"])
    df = df.drop(["pos_1", "pos_2", "pos_3"], axis=1)
    return df


def func_hour8(df):
    df = PD_Technique.dmi_strategy(df, length=14, name="pos_1")
    df = PD_Technique.ema_strategy(df, 5, 60, name="pos_2")
    df = PD_Technique.boll_strategy(df, 50, 1, name="pos_3")
    df["pos"] = df["pos_1"] + df["pos_2"] + df["pos_3"]
    display_last("8h dmi_strategy", df["pos_1"])
    display_last("8h ema_strategy", df["pos_2"])
    display_last("8h boll_strategy", df["pos_3"])
    df = df.drop(["pos_1", "pos_2", "pos_3"], axis=1)
    return df


def func_hour6(df):
    # 3
    df = PD_Technique.dmi_strategy(df, length=14, name="pos_1")
    df = PD_Technique.ema_strategy(df, 5, 60, name="pos_2")
    df = PD_Technique.boll_strategy(df, 50, 1, name="pos_3")
    df["pos"] = df["pos_1"] + df["pos_2"] + df["pos_3"]
    display_last("6h dmi_strategy", df["pos_1"])
    display_last("6h ema_strategy", df["pos_2"])
    display_last("6h boll_strategy", df["pos_3"])
    df = df.drop(["pos_1", "pos_2", "pos_3"], axis=1)
    return df


def func_hour4(df):
    # 9
    df = PD_Technique.boll_strategy(df, 50, 1, name="pos_1")
    df = PD_Technique.ema_strategy(df, 5, 60, name="pos_2")
    df = PD_Technique.dmi_strategy(df, length=14, name="pos_3")
    df = PD_Technique.four_week_strategy(df, n=30, name="pos_6")
    df = PD_Technique.kingkeltner_strategy(df, n=30, name="pos_7")
    df = PD_Technique.ema_slope_trend_follower(df, ma_average_type="EMA", slopeflen=5, slopeslen=21,
                                               trendfilter=True, trendfilterperiod=200, trendfiltertype="EMA",
                                               volatilityfilter=False, volatilitystdevlength=20,
                                               volatilitystdevmalength=30,
                                               name="pos_8")
    df["pos"] = df["pos_1"] + df["pos_2"] + df["pos_3"] + df["pos_6"] + df["pos_7"] + df["pos_8"]
    display_last("4h boll_strategy", df["pos_1"])
    display_last("4h ema_strategy", df["pos_2"])
    display_last("4h dmi_strategy", df["pos_3"])
    display_last("4h four_week_strategy", df["pos_6"])
    display_last("4h kingkeltner_strategy", df["pos_7"])
    display_last("4h ema_slope_trend_follower", df["pos_8"])

    df = df.drop(["pos_1", "pos_2", "pos_3", "pos_6", "pos_7", "pos_8"], axis=1)
    return df


def func_hour2(df):
    # 4
    df = PD_Technique.ema_slope_trend_follower(df, ma_average_type="EMA", slopeflen=5, slopeslen=21,
                                               trendfilter=True, trendfilterperiod=200, trendfiltertype="EMA",
                                               volatilityfilter=False, volatilitystdevlength=20,
                                               volatilitystdevmalength=30,
                                               name="pos_1")
    df["pos"] = df["pos_1"]
    display_last("2h ema_slope_trend_follower", df["pos_1"])
    df = df.drop(["pos_1"], axis=1)
    return df


def download_data(symbol):
    log_service_manager.write_log("[download_data] now go to download symbol:{}".format(symbol))
    b = BinanceClient()
    b.download_save_mongodb(symbol=symbol, _start_datetime=datetime.now() - timedelta(days=90),
                            _end_datetime=datetime.now() + timedelta(days=5), interval=Interval.MINUTE.value)


def run_child(download=False):
    bar_period_factor = [
        # 1
        (func_hour2, 2, Interval.HOUR.value),
        # 6
        (func_hour4, 4, Interval.HOUR.value),
        # 3
        (func_hour6, 6, Interval.HOUR.value),
        # 3
        (func_hour8, 8, Interval.HOUR.value),
        # 3
        (func_hour12, 12, Interval.HOUR.value),
    ]

    # 16 张

    setting = {
        "live_okexs_multi_frame_factor_btc_usd_swap": {
            "class_name": "FactorMultiPeriodV3Strategy",
            "setting": {
                "symbol_pair": "btc_usd_swap",
                "exchange": "OKEX5",
                "vt_symbols_subscribe": [
                    "btc_usd_swap.OKEX5"
                ],
                "class_name": "FactorMultiPeriodV3Strategy",
                "pos": -11,
                "fixed": 1,
                "is_backtesting": False,
                "bar_period_factor": bar_period_factor,
                "exchange_info": {
                    "exchange_name": "OKEX5",
                    "account_key": "OKEX5.BTC-USD-SWAP",
                    "price_tick": 0.01,
                    "volume_tick": 1
                }
            }
        }
    }

    connect_setting = load_json("connect_setting.json")

    all_symbol_pairs = [s["setting"]["symbol_pair"] for s in setting.values()]

    if download:
        log_service_manager.write_log("[run] download data!")
        for ts in all_symbol_pairs:
            ts = ts.replace("_swap", "t")
            download_data(ts)

    event_engine = EventEngine()
    event_engine.start()
    main_engine = MainEngine(event_engine)

    all_exchanges = connect_setting.keys()
    for exchange in all_exchanges:
        if exchange == Exchange.OKEX5.value:
            main_engine.add_gateway(Okex5Gateway)

    cta_app = main_engine.add_app(CtaApp)
    cta_app.init_engine(setting)

    main_engine.write_log("create engine successily!")

    log_engine = main_engine.get_engine("log")

    for exchange, c_setting in connect_setting.items():
        main_engine.connect(c_setting, exchange)

    main_engine.write_log("connect gateway successily!")

    sleep(10)

    print(all_exchanges)
    print(all_symbol_pairs)
    for exchange in all_exchanges:
        for symbol in all_symbol_pairs:
            print(exchange, symbol)
            sub = SubscribeRequest()
            sub.symbol = symbol
            sub.exchange = exchange
            sub.vt_symbol = "{}.{}".format(symbol, exchange)

            main_engine.subscribe(sub, exchange)

    cta_app.init_all_strategies()
    main_engine.write_log("cta_app init finished!")

    sleep(5)

    cta_app.start_all_strategies()

    print("cta_app.start_all_strategies()")

    input()


if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) == 2 and sys.argv[1] == "nd":
        run_child(download=False)
    else:
        run_child(download=True)
