# coding=utf-8

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import talib

from tumbler.object import BarData
from tumbler.constant import Interval
from tumbler.service.mysql_service import MysqlService
from tumbler.function.technique import Technique, PD_Technique


def run():
    '''
    def f(x):
        upper, middle, lower = talib.BBANDS(x, ...) #enter the parameter you need
            return pd.DataFrame({'upper':upper, 'middle':middle, 'lower':lower },
                                index=x.index)
    df[['upper','middle','lower']] = df.groupby('type').apply(lambda x : f(x.x))

    or:

    df["roc30"] = df.groupby(by=['symbol']).apply(
        lambda x: pd.DataFrame(talib.ROC(x.close, 30), index=x.index)
    )

    '''
    mysql_service_manager = MysqlService.get_mysql_service()
    symbols = mysql_service_manager.get_mysql_distinct_symbol(table=MysqlService.get_kline_table(Interval.DAY.value))

    bars = mysql_service_manager.get_bars(symbols=[], period=Interval.DAY.value,
                                          start_datetime=datetime(2017, 1, 1),
                                          end_datetime=datetime.now() + timedelta(hours=10),
                                          sort_way="symbol")

    bars = BarData.suffix_filter(bars, suffix="_usdt")

    # bars = mysql_service_manager.get_bars(symbols=["bnb_usdt"], period=Interval.DAY.value,
    #                                       start_datetime=datetime(2017, 1, 1),
    #                                       end_datetime=datetime.now() + timedelta(hours=10),
    #                                       sort_way="symbol")
    bars.sort()

    df = BarData.get_pandas_from_bars(bars)
    df = df.set_index(["symbol", "datetime"]).sort_index().reset_index()

    # df = PD_Technique.droc(df, 5, field='close', name="DROC5")
    # df = PD_Technique.droc(df, 10, field='close',  name="DROC10")
    # df = PD_Technique.droc(df, 20, field='close', name="DROC20")
    # df = PD_Technique.droc(df, 30, field='close', name="DROC30")
    # df = PD_Technique.droc(df, 60, field='close', name="DROC60")
    # df = PD_Technique.droc(df, 90, field='close', name="DROC90")

    # Technique.er(am.close_array, 20)
    # df["ER10"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.er(x.close, 10), index=x.index)
    # )
    #
    # df["ER20"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.er(x.close, 20), index=x.index)
    # )
    #
    # df["ER30"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.er(x.close, 30), index=x.index)
    # )
    #
    # df["ER60"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.er(x.close, 60), index=x.index)
    # )
    #
    # df["ER90"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.er(x.close, 90), index=x.index)
    # )
    #
    # df["ER120"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.er(x.close, 120), index=x.index)
    # )

    # der
    # df["DER10"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.der(x.close, 10), index=x.index)
    # )
    #
    # df["DER20"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.der(x.close, 20), index=x.index)
    # )
    #
    # df["DER30"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.der(x.close, 30), index=x.index)
    # )
    #
    # df["DER60"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.der(x.close, 60), index=x.index)
    # )
    #
    # df["DER90"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.der(x.close, 90), index=x.index)
    # )
    #
    # df["DER120"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(Technique.der(x.close, 120), index=x.index)
    # )

    #### ROC ###
    # df["ROC1"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ROC(x.close, 1), index=x.index)
    # )
    #
    # df["ROC3"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ROC(x.close, 3), index=x.index)
    # )
    #
    # df["ROC5"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ROC(x.close, 5), index=x.index)
    # )

    # df["ROC10"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ROC(x.close, 10), index=x.index)
    # )
    #
    # df["ROC20"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ROC(x.close, 20), index=x.index)
    # )

    # df["ROC30"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ROC(x.close, 30), index=x.index)
    # )

    # df["ROC60"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ROC(x.close, 60), index=x.index)
    # )
    #
    # df["ROC90"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ROC(x.close, 90), index=x.index)
    # )
    #
    # df["ROC120"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ROC(x.close, 120), index=x.index)
    # )

    #### roc ###
    df["ROC10-5"] = df.groupby(by=['symbol']).apply(
        lambda x: pd.DataFrame(talib.ROC(x.close, 10) - talib.ROC(x.close, 5), index=x.index)
    )

    df["ROC20-10"] = df.groupby(by=['symbol']).apply(
        lambda x: pd.DataFrame(talib.ROC(x.close, 20) - talib.ROC(x.close, 10), index=x.index)
    )

    df["ROC30-10"] = df.groupby(by=['symbol']).apply(
        lambda x: pd.DataFrame(talib.ROC(x.close, 30) - talib.ROC(x.close, 10), index=x.index)
    )

    df["ROC30-20"] = df.groupby(by=['symbol']).apply(
        lambda x: pd.DataFrame(talib.ROC(x.close, 30) - talib.ROC(x.close, 20), index=x.index)
    )

    df["ROC60-30"] = df.groupby(by=['symbol']).apply(
        lambda x: pd.DataFrame(talib.ROC(x.close, 60) - talib.ROC(x.close, 30), index=x.index)
    )

    df["ROC90-60"] = df.groupby(by=['symbol']).apply(
        lambda x: pd.DataFrame(talib.ROC(x.close, 90) - talib.ROC(x.close, 60), index=x.index)
    )

    # df["ROC90"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ROC(x.close, 90), index=x.index)
    # )

    # df["ROC120"] = df.groupby(by=['symbol']).apply(
    #     lambda x: pd.DataFrame(talib.ROC(x.close, 120), index=x.index)
    # )

    mysql_service_manager.replace_factor_from_pd(df, Interval.DAY.value)

    return df


if __name__ == "__main__":
    df = run()
    # df.to_csv(".tumbler/debug3.csv")
    # print(df)
