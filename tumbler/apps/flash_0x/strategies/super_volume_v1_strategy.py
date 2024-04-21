# coding=utf-8

import time
from copy import copy
import math
import random

from mov_sdk.mov_api import MovApi

from tumbler.apps.flash_0x.template import (
    Flash0xTemplate,
)

from tumbler.constant import MAX_PRICE_NUM, Exchange, Direction, Offset
from tumbler.function import get_vt_key
import tumbler.config as config
from tumbler.gateway.mov.base import from_mov_to_system_format_symbol, mov_format_symbol
from tumbler.service import log_service_manager

all_pairs = ["usdc_usdt", "dai_usdt", "dai_usdc"]
xishu_dict = {
    "usdc_usdt": 150,
    "dai_usdt": 130,
    "dai_usdc": 100
}


def get_tiaozheng(symbol, all_money_dict):
    sum = 0
    for key in all_pairs:
        sum += xishu_dict[key] * all_money_dict[key]
    v = xishu_dict[symbol] * all_money_dict[symbol]
    return v / sum


def fx(money):
    x = money / 10000
    if x < 1180:
        return (math.atan((300 - 180) / 80) * 2 / 3.1415926 * 0.03 + 0.075) * 100
    else:
        return ((590.0 / (x - 590.0)) ** 3) * 0.0467 * 100


def get_trading_per_minute(symbol, all_money_dict, fix_money=config.SETTINGS["mov_super_default_volume"]):
    all_money = 0
    for key in all_pairs:
        all_money += all_money_dict[key]

    rate = fx(all_money)
    data = all_money * rate * get_tiaozheng(symbol, all_money_dict) / 100.0 / 365 / 0.0005 / 24 / 60 / fix_money
    # # 输出每分钟刷多少笔
    return 1 / data * 60


def run_get_rate(want_symbol):
    try:
        m = MovApi()
        data = m.get_super_conducting_pool_info()
        ret_dic = {}
        for dic in data["data"]:
            symbol = from_mov_to_system_format_symbol(dic["symbol"])
            total_amount = dic["total_amount"]
            ret_dic[symbol] = float(total_amount)

        return get_trading_per_minute(want_symbol, ret_dic)
    except Exception as ex:
        log_service_manager.write_log("ex:{}".format(ex))
        return -1


def run_get_price(symbol, volume, direction):
    try:
        if direction == Direction.LONG.value:
            side = "buy"
        else:
            side = "sell"
        api = MovApi()
        data = api.get_super_exchange_rate(symbol=mov_format_symbol(symbol), volume=str(volume), side=side)
        price = float(data["data"]["exchange_rate"])
        return price
    except Exception as ex:
        return 0.0


def get_random_volume(max_v):
    try:
        return random.randint(70, max_v)
    except Exception:
        return 100


class SuperVolumeV1Strategy(Flash0xTemplate):
    author = "ipqhjjybj"
    class_name = "SuperVolumeV1Strategy"

    """
    默认参数
    """
    symbol_pair = "btc_usdt"
    vt_symbols_subscribe = []
    target_symbol = "btc"
    base_symbol = "usdt"
    delay_time = 30
    all_working_time = 180
    fixed_amount = config.SETTINGS["mov_super_default_volume"]
    fee_rate = 0.051
    need_work_volume = 0
    target_exchange_info = {
        "exchange_name": Exchange.SUPER.value
    }

    '''
    策略逻辑
        大于火币价格，并且 > 1 + 手续费， 开始卖
        小于火币价格，并且 < 1 + 手续费， 开始买
    '''
    # 参数列表
    parameters = [
        "vt_symbols_subscribe",
        "target_exchange_info",
        "symbol_pair",
        "target_symbol",
        "base_symbol",
        "delay_time"
    ]

    # 需要保存的运行时变量
    variables = ['inited',
                 'trading'
                 ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['target_exchange_info'
                ]

    def __init__(self, mm_engine, strategy_name, settings):
        super(SuperVolumeV1Strategy, self).__init__(mm_engine, strategy_name, settings)
        self.update_target = False
        self.update_account_flag = False

        self.need_work_buy_volume = 0
        self.need_work_sell_volume = 0

        self.target_bids = [(0.0, 0.0)] * MAX_PRICE_NUM  # 缓存的深度数据
        self.target_asks = [(0.0, 0.0)] * MAX_PRICE_NUM  # 缓存的深度数据

        self.pre_time = time.time() - 120
        self.cc_time = time.time() - 1200

        self.working_transfer_request = None

    def on_init(self):
        self.update_account()

    def update_account(self):
        # init
        key_acct_te_target_symbol = get_vt_key(self.target_exchange_info["exchange_name"], self.target_symbol)
        key_acct_te_base_symbol = get_vt_key(self.target_exchange_info["exchange_name"], self.base_symbol)

        acct_te_target = self.get_account(key_acct_te_target_symbol, self.target_exchange_info["address"])
        acct_te_base = self.get_account(key_acct_te_base_symbol, self.target_exchange_info["address"])
        if acct_te_target is not None and acct_te_base is not None:
            self.target_exchange_info["pos_target_symbol"] = acct_te_target.balance
            self.target_exchange_info["pos_base_symbol"] = acct_te_base.balance
            self.write_log("[update_account] target_exchange_info:{}".format(self.target_exchange_info))
            if not self.update_account_flag:
                self.update_account_flag = True
        else:
            self.write_log("[update_account] acct_te_target is None, key:{}".format(key_acct_te_target_symbol))

    def on_start(self):
        self.write_log("{} is now starting".format(self.strategy_name))

    def on_stop(self):
        self.write_log("{} is stop".format(self.strategy_name))

    def compute(self):
        now_time = time.time()
        if now_time > self.pre_time + self.delay_time:
            t_rate = run_get_rate(self.symbol_pair)
            if t_rate > 0:
                self.delay_time = t_rate
            self.write_log("bids:{} asks:{} fixed_amount:{} delay_time:{}".format(self.target_bids[0][0],
                                                                                  self.target_asks[0][0],
                                                                                  self.fixed_amount, self.delay_time))

            if self.target_exchange_info["pos_base_symbol"] >= self.fixed_amount and \
                    self.target_exchange_info["pos_base_symbol"] >= self.target_exchange_info["pos_target_symbol"]:

                self.need_work_buy_volume += self.fixed_amount
                # self.send_order(self.symbol_pair, Exchange.SUPER.value, Direction.LONG.value, Offset.OPEN.value,
                #                 self.target_bids[0][0], self.fixed_amount)
                if now_time > self.all_working_time + self.cc_time:
                    now_volume = get_random_volume(self.need_work_buy_volume)
                    price = run_get_price(self.symbol_pair, now_volume, Direction.LONG.value)
                    self.send_order(self.symbol_pair, Exchange.SUPER.value, Direction.LONG.value, Offset.OPEN.value,
                                    price, now_volume)

                    self.need_work_buy_volume -= now_volume
                    self.cc_time = now_time

                self.pre_time = now_time

            elif self.target_exchange_info["pos_target_symbol"] >= self.fixed_amount and \
                    self.target_exchange_info["pos_target_symbol"] >= self.target_exchange_info["pos_base_symbol"]:

                self.need_work_sell_volume += self.fixed_amount
                # self.send_order(self.symbol_pair, Exchange.SUPER.value, Direction.SHORT.value, Offset.OPEN.value,
                #                 self.target_asks[0][0], self.fixed_amount)
                #
                if now_time > self.all_working_time + self.cc_time:
                    now_volume = get_random_volume(self.need_work_sell_volume)
                    price = run_get_price(self.symbol_pair, now_volume, Direction.SHORT.value)
                    self.send_order(self.symbol_pair, Exchange.SUPER.value, Direction.SHORT.value, Offset.OPEN.value,
                                    price, now_volume)
                    self.need_work_sell_volume -= now_volume
                    self.cc_time = now_time

                self.pre_time = now_time

    def on_tick(self, tick):
        if tick.exchange == self.target_exchange_info["exchange_name"]:
            if tick.bid_prices[0] > 0:
                new_target_bids, new_target_asks = tick.get_depth()

                if new_target_bids:
                    self.target_bids = new_target_bids
                if new_target_asks:
                    self.target_asks = new_target_asks

                if self.update_account_flag and self.trading:
                    self.compute()
                else:
                    self.update_account()

    def on_order(self, order):
        pass

    def on_trade(self, trade):
        pass

    def on_flash_account(self, account):
        '''
        :param account:
        :return:
        更新 flash 这边情况信息
        '''
        if account.account_id == self.target_symbol:
            if "pos_target_symbol" in self.target_exchange_info.keys():
                bef_pos_target_symbol = self.target_exchange_info["pos_target_symbol"]
            else:
                bef_pos_target_symbol = account.balance
            new_traded_target_symbol = bef_pos_target_symbol - account.balance
            if new_traded_target_symbol > 0:
                self.write_important_log(
                    "[new traded target] new_traded_target_symbol:{}, bef_pos_target_symbol:{}, balance:{}".format(
                        new_traded_target_symbol, bef_pos_target_symbol, account.balance))

            self.target_exchange_info["pos_target_symbol"] = account.balance
        elif account.account_id == self.base_symbol:
            if "pos_base_symbol" in self.target_exchange_info.keys():
                bef_pos_base_symbol = self.target_exchange_info["pos_base_symbol"]
            else:
                bef_pos_base_symbol = account.balance
            new_traded_base_symbol = bef_pos_base_symbol - account.balance
            if new_traded_base_symbol > 0:
                self.write_important_log(
                    "[new traded base] new_traded_base_symbol:{}, bef_pos_base_symbol:{}, balance:{}".format(
                        new_traded_base_symbol, bef_pos_base_symbol, account.balance))

            self.target_exchange_info["pos_base_symbol"] = account.balance

        if "pos_base_symbol" in self.target_exchange_info.keys() and "pos_target_symbol" in self.target_exchange_info.keys():
            self.update_account_flag = True

        self.write_log("[on_flash_account] account account_id:{} balance:{} balance:{}".
                       format(account.account_id, account.available, account.balance))

    def on_transfer(self, transfer_req):
        msg = "[process_transfer_event] :{}".format(transfer_req.__dict__)
        self.write_important_log(msg)

        if self.working_transfer_request:
            msg = "[process_transfer_event] already has transfer req! drop it!"
            self.write_important_log(msg)
            return

        if transfer_req.from_exchange != Exchange.SUPER.value:
            msg = "[process_transfer_event] from_exchange is not super ! drop it! "
            self.write_important_log(msg)
            return

        if transfer_req.asset_id != self.target_symbol and transfer_req.asset_id != self.base_symbol:
            msg = "[process_transfer_event] asset_id:{} not existed".format(transfer_req.asset_id)
            self.write_important_log(msg)
            return

        self.working_transfer_request = copy(transfer_req)
        self.process_transfer()

    def go_transfer(self):
        if self.working_transfer_request:
            transfer_id = self.transfer_amount(self.working_transfer_request)
            msg = "[process_transfer] transfer_amount result:{}".format(transfer_id)
            self.write_important_log(msg)
            return transfer_id

    def process_transfer(self):
        if not self.working_transfer_request:
            return

        if self.working_transfer_request:
            now = time.time()
            # 超过一分钟的转账请求，丢弃掉
            if now - self.working_transfer_request.timestamp > 60:
                msg = "[process_transfer] drop working_transfer request for time exceed! a:{},b:{}". \
                    format(now, self.working_transfer_request.timestamp)
                self.write_important_log(msg)
                self.working_transfer_request = None
                return

        if self.working_transfer_request.asset_id in [self.target_symbol, self.base_symbol]:
            transfer_id = self.go_transfer()
            if transfer_id:
                msg = "[process_transfer]transfer_amount:{}".format(self.working_transfer_request.transfer_amount)
                self.write_important_log(msg)

                self.working_transfer_request = None
            else:
                now = time.time()
                if now - self.working_transfer_request.timestamp > 20:
                    msg = "[process_transfer] send drop working_transfer request for time exceed! a:{},b:{}".format(
                        now, self.working_transfer_request.timestamp)
                    self.write_important_log(msg)
                    self.working_transfer_request = None
