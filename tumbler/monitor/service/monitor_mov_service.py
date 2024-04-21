# coding=utf-8
import time
import os
from copy import copy
from collections import defaultdict

from mov_sdk.mov_api import MovApi

from tumbler.function import read_all_lines
from tumbler.gateway.mov.base import mov_format_symbol
from tumbler.record.huobi_client import HuobiClient
from tumbler.record.mov_client import MovClient
from tumbler.service.log_service import log_service_manager
from tumbler.service.dingtalk_service import ding_talk_service

'''
setting_file = {
    "server_name": "server001",
    "flash_check_symbols": ["btm_usdt", "eth_usdt", "btm_eth", "btm_btc", "eth_btc", "btc_usdt"],
    "mov_check_symbols": ["btm_usdt", "eth_usdt", "btm_eth", "btm_btc", "eth_btc", "btc_usdt"],
    "huobi_check_symbols": ["btm_usdt", "eth_usdt", "btm_eth", "btm_btc", "eth_btc", "btc_usdt"],
    "init_balance": {
        "btc": 18.9,
        "btm": 5450000,
        "eth": 403,
        "usdt": 115000,
        "ht": 1200,
        "sup": 102,
        "ltc": 300
    },
    "record_path": "/home/admin/tumbler/run_tumbler/transfer/.tumbler/record.log"
    "sleep_seconds": 60
}
'''


class MonitorService(object):
    """
    1、检测MOV数据是否正确
    2、检测火币数据是否正确
    3、检测资金余额是否发生了偏移
    """
    def __init__(self, setting):
        self.huobi_client = HuobiClient("", "")
        self.mov_client = MovClient("", "")
        self.mov_api = MovApi(secret_key="")
        self.flash_check_symbols = setting.get("flash_check_symbols", [])
        self.mov_check_symbols = setting.get("mov_check_symbols", [])
        self.huobi_check_symbols = setting.get("huobi_check_symbols", [])
        self.server_name = setting.get("server_name", "no_name_server")
        self.sleep_seconds = setting.get("sleep_seconds", 60)
        self.init_balance = setting.get("init_balance", {})
        self.record_path = setting.get("record_path", "/home/admin/tumbler/run_tumbler/transfer/.tumbler/record.log")

        self.error_func_dict = defaultdict(int)
        self.save_ticker = {}

    def output_msg(self, msg):
        ding_talk_service.send_msg(msg)
        log_service_manager.write_log(msg)

    def check_flash_depth(self):
        try:
            for symbol in self.flash_check_symbols:
                tick = self.huobi_client.get_ticker(symbol)
                bid_price = tick.bid_prices[0]
                ask_price = tick.ask_prices[0]

                data = self.mov_api.get_flash_depth(mov_format_symbol(symbol))
                asks = data["data"]["asks"]
                bids = data["data"]["bids"]

                target_ask = float(asks[0][0])
                target_bid = float(bids[0][0])

                key = "check_flash_depth.{}".format(symbol)
                if target_ask >= ask_price and target_bid <= bid_price:
                    self.error_func_dict[key] = 0
                    log_service_manager.write_log("[check_flash_depth] server_name:{} symbol:{} right!"
                                                  .format(self.server_name, symbol))
                else:
                    self.error_func_dict[key] += 1
                    if self.error_func_dict[key] > 5:
                        self.output_msg(
                            "[check_flash_depth] server_name:{} symbol:{} error! target_ask:{} target_bid:{}"
                                .format(self.server_name, symbol, target_ask, target_bid))

        except Exception as ex:
            self.output_msg("[check_flash_depth] server:{} ex:{}".format(self.server_name, ex))

    def check_mov_depth(self):
        try:
            for symbol in self.mov_check_symbols:
                tick = self.mov_client.get_ticker(symbol)

                key = "check_mov_depth.{}".format(symbol)
                if tick.bid_prices[0] > 0 and tick.ask_prices[0] > 0:
                    if tick.ask_prices[0] > tick.bid_prices[0] * 1.05:
                        self.error_func_dict[key] += 1
                        if self.error_func_dict[key] > 5:
                            self.output_msg("[check_mov_depth] server_name:{} symbol:{} ask_price:{} bid_price:{}"
                                            " not right!"
                                            .format(self.server_name, symbol, tick.ask_prices[0], tick.bid_prices[0]))
                    else:
                        self.error_func_dict[key] = 0
                else:
                    self.output_msg("[check_mov_depth] server_name:{} symbol:{} tick empty!"
                                    .format(self.server_name, symbol))
        except Exception as ex:
            self.output_msg("[check_mov_depth] server:{} ex:{}".format(self.server_name, ex))

    def get_last_ticker_num(self, vt_symbol):
        ticker = self.save_ticker.get(vt_symbol, None)
        if ticker is not None:
            return ticker.get_depth_unique_val()
        else:
            return 0

    def check_huobi_depth(self):
        try:
            for symbol in self.mov_check_symbols:
                key = "check_huobi_depth.{}".format(symbol)
                ticker = self.huobi_client.get_ticker(symbol)
                now_ticker_num = ticker.get_depth_unique_val()
                last_ticker_num = self.get_last_ticker_num(ticker.vt_symbol)
                if now_ticker_num == last_ticker_num:
                    self.error_func_dict[key] += 1
                    if self.error_func_dict[key] > 5:
                        self.output_msg("[check_huobi_depth] server_name:{} symbol:{} ticker is same!"
                                        .format(self.server_name, symbol))

                else:
                    self.error_func_dict[key] = 0

                self.save_ticker[ticker.vt_symbol] = copy(ticker)

        except Exception as ex:
            self.output_msg("[check_huobi_depth] server_name:{} ex:{}".format(self.server_name, ex))

    def check_mov_no_gateway(self):
        try:
            right = 0
            for i in range(5):
                data = self.mov_api.get_depth("BTC/USDT")
                if data and "code" in data.keys():
                    right += 1
            if right != 5:
                self.output_msg("[check_mov_no_gateway] right not equal!")
        except Exception as ex:
            self.output_msg("[check_mov_no_gateway] server_name:{} ex:{}".format(self.server_name, ex))

    def detect_word(self, path, word):
        flag = False
        f = open(path, "r")
        for line in f:
            if word in line:
                flag = True
                break
        f.close()
        return flag

    def find_protect_word(self):
        try:
            # super:["btm_btc", "eth_btc", "btm_eth"]
            for symbol in ["btm_btc", "eth_btc", "btm_eth"]:
                path = "tumbler/run_tumbler/super/v2/arbitrary/multi_q/{}/.tumbler/strategy_run_log" \
                       "/movsuper_v3_{}_base_huobi_bigger.log".format(symbol, symbol)
                if os.path.exists(path):
                    flag = self.detect_word(path, "protect 1")
                    if flag:
                        msg = "[find_protect_word] symbol:{} path:{} protect 1".format(symbol, path)
                        self.output_msg(msg)

            for symbol in ["btm_ltc", "sup_btm", "sup_ltc"]:
                path = "/home/admin/tumbler/run_tumbler/super/v2/arbitrary/triangle_multi_q/{}" \
                       "/.tumbler/strategy_run_log/movsuper_v4_{}_base_huobi_bigger.log".format(symbol, symbol)
                if os.path.exists(path):
                    flag = self.detect_word(path, "protect 1")
                    if flag:
                        msg = "[find_protect_word] symbol:{} path:{} protect 1".format(symbol, path)
                        self.output_msg(msg)

        except Exception as ex:
            self.output_msg("[find_protect_word] server_name:{} ex:{}".format(self.server_name, ex))

    def check_account(self):
        try:
            if os.path.exists(self.record_path):
                lines = read_all_lines(self.record_path)
                if len(lines) > 6:
                    first = lines[0]
                    all_symbols = first.split(',')[1:]
                    v_arr = []
                    for key in all_symbols:
                        v = self.init_balance.get(key, 0)
                        v_arr.append(v)
                    lines = lines[-6:]
                    ok = [0] * len(all_symbols)
                    for line in lines:
                        arr = line.split(',')[1:]
                        for i in range(len(arr)):
                            if float(arr[i]) >= v_arr[i] * 0.98:
                                ok[i] = 1

                    for i in range(len(ok)):
                        if not ok[i]:
                            symbol = all_symbols[i]
                            msg = "Error in check_account, {} {} is lack!".format(self.server_name, symbol)
                            self.output_msg(msg)
                            return
                    log_service_manager.write_log("not_lack")

        except Exception as ex:
            self.output_msg("[check_account] server_name:{} ex:{}".format(self.server_name, ex))

    def run(self):
        while True:
            try:
                self.check_flash_depth()
                self.check_mov_depth()
                self.check_mov_no_gateway()
                self.find_protect_word()
                self.check_huobi_depth()
                self.check_account()
            except Exception as ex:
                log_service_manager.write_log("[run] server_name:{} ex:{}".format(self.server_name, ex))
            time.sleep(self.sleep_seconds)
