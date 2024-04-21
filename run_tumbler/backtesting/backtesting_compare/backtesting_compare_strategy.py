# coding=utf-8

from datetime import datetime
from tumbler.constant import Interval

from tumbler.apps.backtester.backtesting import BacktestingEngine, BacktestingMode
from tumbler.apps.cta_strategy.strategies.compare_strategy import CompareStrategy
from tumbler.function.technique import PD_Technique


def func_hour(df):
    # 7
    #df = PD_Technique.ema_strategy(df, 5, 15, name="pos")
    df = PD_Technique.dmi_strategy(df, 14, name="pos")
    #df = PD_Technique.boll_strategy(df, 50, 1, name="pos")
    # df = PD_Technique.ema_strategy(df, 5, 20, name="pos_2")
    # df = PD_Technique.three_line_strategy(df, 5, 10, 20, name="pos_3")
    # df = PD_Technique.four_week_strategy(df, n=40, name="pos_4")
    # df["pos"] = df["pos_1"] * 4 + df["pos_2"] + df["pos_3"] + df["pos_4"]
    # df = df.drop(["pos_1", "pos_2", "pos_3", "pos_4"], axis=1)
    return df


def run():
    # symbol, sllippage, rate = "eth_usdt", 0.1, 0.001
    # symbol, sllippage, rate = "btc_usdt", 2, 0.001
    symbol, sllippage, rate = "eth_btc", 0, 0.001
    vt_symbol = "{}.BINANCE".format(symbol)

    setting = {
        "is_backtesting": True,
        "func": func_hour,
        "exchange_info": {
            "exchange_name": "OKEXS",
            "account_key": "OKEXS.BTC-USD-SWAP",
            "price_tick": 0.00000001
        }
    }

    engine = BacktestingEngine()
    engine.set_parameters(vt_symbol=vt_symbol, interval=Interval.MINUTE.value,
                          start=datetime(2017, 7, 1, 8, 10), rate=rate,
                          slippage=sllippage, size=1, price_tick=0.000001,
                          capital=0, end=datetime(2022, 6, 29, 9, 50),
                          mode=BacktestingMode.BAR.value)

    filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_hour1.csv".format(
        symbol)
    # filename = "/Users/szh/git/personal_tumbler/run_tumbler/tools/produce_csv_data/.tumbler/fix_{}_min1.csv".format(
    #     symbol)

    engine.add_strategy(CompareStrategy, setting)
    engine.load_data(filename=filename)
    engine.run_backtesting()
    df = engine.calculate_result()
    engine.calculate_statistics()

    engine.show_chart()

    input()


if __name__ == "__main__":
    run()
