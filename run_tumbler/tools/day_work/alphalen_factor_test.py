# coding=utf-8

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import alphalens as al

from tumbler.object import BarData, FactorData
from tumbler.constant import Interval
from tumbler.service.mysql_service import MysqlService


def create_factor_data_from_mysql():
    mysql_service_manager = MysqlService.get_mysql_service()
    factor_ret = mysql_service_manager.get_factors(
        factor_code="beta_365",
        interval=Interval.DAY.value,
        start_dt=datetime(2017, 1, 1),
        end_dt=datetime.now()
    )
    factor_ret = FactorData.suffix_filter(factor_ret, suffix="_usdt")

    factor_df = FactorData.make_alphalen_factor_df(factor_ret, zscore=True)
    print(factor_df)

    bars_ret = mysql_service_manager.get_bars(
        symbols=[],
        period=Interval.DAY.value,
        start_datetime=datetime(2017, 1, 1),
        end_datetime=datetime.now(),
        sort_way="symbol")
    bars_ret = BarData.suffix_filter(bars_ret, suffix="_usdt")
    price_df = BarData.make_alphalen_price_df(bars_ret)
    print(price_df)

    return al.utils.get_clean_factor_and_forward_returns(factor_df, price_df, max_loss=0.99)


factor_data = create_factor_data_from_mysql()
print(factor_data)
