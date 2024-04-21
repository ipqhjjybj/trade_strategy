# coding=utf-8

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import talib

from tumbler.object import BarData
from tumbler.constant import Interval
from tumbler.service.mysql_service import MysqlService, log_service_manager
from tumbler.function.technique import Technique, PD_Technique


def run_alpha():
    mysql_service = MysqlService()

    conn = mysql_service.get_conn()
    sqll = "select distinct factor_code from factor_1day"

    cur = conn.cursor()
    cur.execute(sqll)

    my_result = cur.fetchall()

    for factor_code, in list(my_result):
        upper_code = str(factor_code).upper()

        if factor_code != upper_code:
            new_sql = f"update factor_1day set `factor_code`='{upper_code}' where `factor_code`='{factor_code}'"

            print(new_sql)
            cur.execute(new_sql)
            conn.commit()

    cur.close()
    conn.close()


if __name__ == "__main__":
    run_alpha()
