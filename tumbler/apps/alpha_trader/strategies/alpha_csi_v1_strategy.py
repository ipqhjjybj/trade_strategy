import time
from copy import copy
from collections import defaultdict
from enum import Enum
import os
from datetime import datetime, timedelta

from tumbler.apps.alpha_trader.template import (
    AlphaTemplate
)

from tumbler.constant import MAX_PRICE_NUM, Exchange, DiffTypeFuture
from tumbler.object import ContractData
from tumbler.function import get_vt_key, get_round_order_price, simple_load_json, get_dt_use_timestamp
from tumbler.constant import Direction, Status, Offset
from tumbler.function import get_two_currency
import tumbler.function.risk as risk
from tumbler.function.future_manager import UsdtContractManager
from tumbler.apps.cta_strategy.template import NewOrderSendModule
from tumbler.apps.alpha_trader.template import DataFetchTemplate
from tumbler.constant import Interval


class DayWorkStrategy(DataFetchTemplate):
    '''
    def func(df, name):
        df[name] = talib.ROC(df.close, 10)
        return df
    '''
    def __init__(self, strategy_name, exchange, func, keep_num, reverse=True):
        super(DayWorkStrategy, self).__init__(strategy_name)

        self.exchange = exchange
        self.future_manager = UsdtContractManager(self.exchange, self)

        self.keep_num = keep_num
        self.func = func
        self.reverse = reverse

        self.now_day_long_coins = []
        self.now_day_short_coins = []

        self.pre_day_long_coins = []
        self.pre_day_short_coins = []

        self.pre_day = -1

        self.time_day_work = risk.TimeWork(60)

    def day_work(self):
        now = datetime.now()
        if now.hour >= 8 and now.day != self.pre_day and self.time_day_work.can_work():
            str_now = now.strftime("%Y-%m-%d")
            self.future_manager.run_update()
            symbols = self.future_manager.get_all_symbols()
            self.write_log("[day_work] symbols:{}".format(symbols))
            # pd_data = self.mysql_service_manager.get_bars_to_pandas_data(
            #     symbols=symbols,  period=Interval.DAY.value,
            #     start_datetime=datetime.now() - timedelta(days=80), end_datetime=datetime.now())
            pd_data = self.binance_client.get_bars_to_pandas_data(
                symbols=symbols, period=Interval.DAY.value,
                start_datetime=datetime.now() - timedelta(days=80), end_datetime=datetime.now())

            all_dates = list(set(list(pd_data.datetime)))
            all_dates.sort()

            to_sort_values = []
            for symbol in symbols:
                df = copy(pd_data[pd_data.symbol == symbol])
                if len(df.index) > 30:
                    df = self.func(df, "val")
                    if str(list(df["datetime"])[-1])[:10] == str_now:
                        to_sort_values.append((df["val"][df.index[-1]], symbol))
                    else:
                        self.write_log(f"[day_work] bars not updated! go return! df:{df}")
                        return

            self.pre_day = now.day
            self.write_log(f"[day_work] bars all get! {now}")

            to_sort_values.sort(reverse=self.reverse)

            self.write_log(f"[day_work] to_sort_values:{to_sort_values}")

            self.pre_day_long_coins = copy(self.now_day_long_coins)
            self.pre_day_short_coins = copy(self.now_day_short_coins)

            self.now_day_long_coins = copy([x[1] for x in to_sort_values[:self.keep_num]])
            self.now_day_short_coins = copy([x[1] for x in to_sort_values[-self.keep_num:]])

            self.write_log(f"[day_work] now:{now} now_day_long_coins:{self.now_day_long_coins}")
            self.write_log(f"[day_work] now:{now} now_day_short_coins:{self.now_day_short_coins}")

        return self.now_day_long_coins, self.now_day_short_coins


class AlphaCsiV1Strategy(AlphaTemplate):
    '''
    V1 是采用双进程方式来做
    日级别数据其他地方来处理
    小时级别数据内存中处理
    '''
    author = "ipqhjjybj"
    class_name = "AlphaCsiV1Strategy"

    contract_exchange = Exchange.HUOBIU.value

    # 参数列表
    parameters = [
        'strategy_name',
        'class_name',
        'author',
        'contract_exchange',
        'fixed'  # 固定大小的下单USDT数量
    ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(AlphaCsiV1Strategy, self).__init__(mm_engine, strategy_name, settings)

        self.contract_tick_dict = {}
        self.future_manager = UsdtContractManager(self.contract_exchange, self)
        self.order_module_dict = {}
        self.today_long_coins = []
        self.today_short_coins = []
        self.pre_day_long_coins = []
        self.pre_day_short_coins = []

        self.time_read_coins_time = risk.TimeWork(60)
        self.day_work_coin_file = "./tumbler/day_work.json"
        '''
        # day_work_coin_file
        {
            "code": 0,
            "long_stocks": [btc_usdt],
            "short_stocks": [eth_usdt],
            "time": 1626138990
        }
        '''

        self.bef_update_day = -1

    def on_init(self):
        self.write_log("[on_init]")

    def on_start(self):
        self.write_log("[on_start]")

        self.update_contracts()

    def on_stop(self):
        self.write_log("[on_stop]")

    def update_contracts(self):
        self.future_manager.run_update()

    def update_account(self):
        self.write_log("[update_account]")

    def alpha_subscribe(self, coin):
        self.write_log("[alpha_subscribe] go to subscribe coin:{}".format(coin))

    def alpha_unsubscribe(self, coin):
        self.write_log("[alpha_unsubscribe] go to unsubscribe coin:{}".format(coin))

    def update_day_work(self):
        '''
        每天更新今天需要交易的票
        '''
        now = datetime.now()
        if self.time_read_coins_time.can_work() and 8 <= now.hour and self.bef_update_day != now.day:
            try:
                self.write_log("[update_day_work] go to update day coin!")
                js_data = simple_load_json(self.day_work_coin_file)
                if js_data:
                    code = js_data.get("code", -1)
                    long_coins = js_data.get("long_stocks", [])
                    short_coins = js_data.get("short_stocks", [])
                    timestamp = js_data.get("time", 0)
                    if int(code) == 0 and len(long_coins) + len(short_coins) > 0 and timestamp > 0:
                        dt = get_dt_use_timestamp(timestamp, 1)
                        if dt.day == now.day:
                            self.pre_day_long_coins = copy(self.today_long_coins)
                            self.pre_day_short_coins = copy(self.today_short_coins)

                            self.today_long_coins = copy(long_coins)
                            self.today_short_coins = copy(short_coins)

                            self.write_log("[update_day_work] today_long_coins:{}".format(self.today_long_coins))
                            self.write_log("[update_day_work] today_short_coins:{}".format(self.today_short_coins))

                            td_coins = set(self.today_long_coins + self.today_short_coins)
                            pd_coins = set(self.pre_day_long_coins + self.pre_day_short_coins)
                            to_subscribe_coins = [x for x in td_coins if x not in pd_coins]
                            to_unsubscribe_coins = [x for x in pd_coins if x not in td_coins]

                            for coin in to_subscribe_coins:
                                self.alpha_subscribe(coin)
                            for coin in to_unsubscribe_coins:
                                self.alpha_unsubscribe(coin)

                            self.bef_update_day = now.day
                        else:
                            msg = "[update_day_work] read dt:{} not equal now.day:{}!" \
                                .format(dt, now.day)
                            self.write_log(msg)
                            self.write_important_log(msg)
                    else:
                        msg = "[update_day_work] coins file empty! status:{} len:{} timestamp:{}" \
                            .format(code, len(long_coins) + len(short_coins), timestamp)
                        self.write_log(msg)
                else:
                    msg = "[update_day_work] not found day work coin!"
                    self.write_log(msg)
            except Exception as ex:
                msg = f"[update_day_work] Error in update day coin! ex:{ex}"
                self.write_log(msg)
                self.write_important_log(msg)

    def compute_target_positions(self):
        pass

    def on_tick(self, tick):
        pass

    def on_bbo_tick(self, bbo_ticker):
        pass

    def on_order(self, order):
        pass

    def on_trade(self, trade):
        self.write_log('[on_trade] start')
        msg = '[trade detail] :{}'.format(trade.__dict__)
        self.write_important_log(msg)
        self.send_ding_msg(msg)
