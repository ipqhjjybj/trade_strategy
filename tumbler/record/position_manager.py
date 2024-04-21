# encoding: UTF-8

import json
import os
from datetime import datetime

from tumbler.function import FilePrint
from tumbler.record.client_quick_query import ClientPosPriceQuery
from tumbler.constant import Exchange


class MMPositionManager(object):
    '''
    数字货币持仓 管理类
    1. 统计某人账户下面的  总持仓合计, 分持仓等
    '''

    setting_position_filename = "position_setting.json"

    def __init__(self, _setting_position_filename="position_setting.json"):
        self.setting_position_filename = _setting_position_filename

        self.all_assets = {}  # "people" : {"tnb" : {"OKEX": 300.0 , "BINANCE":300.0 , "all":600.0 } }

    def judge_assets_enough(self, key_name, exchange_assets, require_min_assets):
        arr = []
        for asset in require_min_assets.keys():
            val = exchange_assets.get(asset, 0)
            if 0 < val < require_min_assets[asset]:
                arr.append((key_name, asset))
        return arr

    def deal_setting(self, setting):
        people = setting["people"]
        assets = setting["assets"]
        accounts = setting["accounts"]

        require_info_arr = []
        info_dic = {}
        for account in accounts:
            key_name = account["key_name"]
            exchange = account["exchange"]

            api_key = account["api_key"]
            secret_key = account["secret_key"]
            passphrase = account.get("passphrase", "")
            third_address = account.get("third_address", "")
            third_public_key = account.get("third_public_key", "")

            if exchange == Exchange.FLASH.value:
                exchange_assets = ClientPosPriceQuery.query_all_assets(Exchange.MOV.value, api_key, secret_key,
                                                                       third_address=third_address,
                                                                       third_public_key=third_public_key)
            else:
                exchange_assets = ClientPosPriceQuery.query_all_assets(exchange, api_key, secret_key, passphrase,
                                                                       third_address=third_address,
                                                                       third_public_key=third_public_key)

            require_min_assets = account.get("require_min_assets", {})
            require_arr = self.judge_assets_enough(key_name, exchange_assets, require_min_assets)
            need_assets = [x[1] for x in require_arr]
            account["need_assets"] = need_assets

            require_info_arr = require_info_arr + require_arr

            account["all_assets"] = exchange_assets

            for e_asset in exchange_assets.keys():
                val = float(exchange_assets[e_asset])
                if e_asset in assets:
                    if e_asset in info_dic.keys():
                        try:
                            info_dic[e_asset][exchange] += val
                        except Exception as ex:
                            info_dic[e_asset][exchange] = val
                        try:
                            info_dic[e_asset]["all"] += val
                        except Exception as ex:
                            info_dic[e_asset]["all"] = val
                            print("Error in deal_setting computing . Setting : {}".format(setting))
                    else:
                        info_dic[e_asset] = {exchange: val, "all": val}

        self.all_assets[people] = info_dic
        return self.all_assets, require_info_arr, accounts

    def load_pattern_path(self, compute_symbol_value_list=[]):
        with open(self.setting_position_filename) as f:
            l = json.load(f)

            for setting in l:
                self.deal_setting(setting)

        self.output_every_day_account()

        # for base_symbol in compute_symbol_value_list:
        #     self.output_value_day_account(base_symbol)

    def output_every_day_account(self):
        if os.path.exists("./accounts") is False:
            os.mkdir("./accounts")

        today = datetime.now().strftime("%Y-%m-%d")
        day_dir = "./accounts/" + today

        if os.path.exists(day_dir) is False:
            os.mkdir(day_dir)

        for people in self.all_assets.keys():
            file = day_dir + "/" + people + ".log"
            people_assets = self.all_assets[people]

            asset_keys = list(people_assets.keys())
            asset_keys.sort()

            msg_arr = []
            for asset in asset_keys:
                asset_volume_dict = people_assets[asset]

                volume_msg_arr = [asset + "_all:" + str(asset_volume_dict["all"])]

                exchange_arrs = list(asset_volume_dict.keys())
                exchange_arrs.sort()
                for exchange in exchange_arrs:
                    if exchange != "all":
                        volume_msg_arr.append(asset + "_" + str(exchange) + ":" + str(asset_volume_dict[exchange]))

                msg_arr.append(','.join(volume_msg_arr))
            msg = ','.join(msg_arr)

            f = open(file, "a")
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ":::" + people + "::" + "[" + msg + "]" + "\n")
            f.close()

    def output_value_day_account(self, base_symbol):
        base_dir = "./{}_account".format(base_symbol)
        if os.path.exists(base_dir) is False:
            os.mkdir(base_dir)

        today = datetime.now().strftime("%Y-%m-%d")
        day_dir = base_dir + "/" + today

        if os.path.exists(day_dir) is False:
            os.mkdir(day_dir)

        if os.path.exists("./temp") is False:
            os.mkdir("./temp")

        logger = FilePrint("%s.log" % (base_symbol + "_" + today), "temp", "w")

        for people in self.all_assets.keys():
            file = day_dir + "/" + people + ".log"
            people_assets = self.all_assets[people]

            people_total_value = 0.0

            for asset in people_assets.keys():
                if asset not in ["okdk", "hbpoint"]:
                    asset_volume_dict = people_assets[asset]

                    all_volume = asset_volume_dict["all"]

                    price = ClientPosPriceQuery.query_pure_price(asset + "_" + base_symbol)
                    if price is not None:
                        people_total_value += float(price) * float(all_volume)
                    else:
                        msg_info = "people:{} asset:{} base_symbol:{}".format(people, asset, base_symbol)
                        logger.write(msg_info)

            f = open(file, "w")
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "," + str(people_total_value) + "\n")
            f.close()

        logger.close()

    def daily_run(self, compute_symbol_value_list=[]):
        self.load_pattern_path(compute_symbol_value_list=compute_symbol_value_list)

    def parse_line(self, line):
        try:
            ret_dic = {}
            str_datetime, content = line.strip().split(':::')
            str_people, assets_content = content.split('::')
            assets_content = assets_content[1:-1]
            assets_arr = assets_content.split(',')
            for assets_str in assets_arr:
                asset, value = assets_str.split(':')
                ret_dic[asset] = float(value)
            return ret_dic, str_datetime, str_people
        except Exception as ex:
            return {}, "", ""

    def people_figure_plot_people_name(self, people_name):
        if os.path.exists("./accounts") is False:
            print("dir accounts is not exists")
        else:
            dirs = os.listdir("./accounts")
            dirs.sort()
            arr_lines = []
            for fileDir in dirs:
                file_path_dir = "./accounts/" + fileDir

                if os.path.exists(file_path_dir) is True:
                    file_path = file_path_dir + "/" + people_name + ".log"
                    if os.path.exists(file_path) is True:
                        f = open(file_path, "r")
                        pre_line = ""
                        for line in f:
                            pre_line = line.strip()

                        if len(pre_line) > 0:
                            arr_lines.append(pre_line)
                        f.close()
                else:
                    print("dir %s is not exists" % (str(file_path_dir)))

            if len(arr_lines) > 0:
                if os.path.exists("./results") is False:
                    os.mkdir("./results")

                file_path = "./results/" + people_name + ".log"

                f = open(file_path, "w")
                for line in arr_lines:
                    f.write(line + "\n")
                f.close()

                if os.path.exists("./results_minus") is False:
                    os.mkdir("./results_minus")

                file_path = "./results_minus/" + people_name + ".log"
                f = open(file_path, "w")

                pre_assets = {}
                for line in arr_lines:
                    parse_assets, str_datetime, str_people = self.parse_line(line)
                    if pre_assets:
                        all_has_asts = list(set(list(parse_assets.keys()) + list(pre_assets.keys())))
                        arr_str = []
                        for asset in all_has_asts:
                            if 'all' in asset:
                                pre_value = pre_assets.get(asset, 0.0)
                                now_value = parse_assets.get(asset, 0.0)

                                sstr = asset + ":" + str(now_value - pre_value)
                                arr_str.append(sstr)

                        st_line = ','.join(arr_str)
                        st_line = str_datetime + ":::" + str_people + "::" + st_line
                        f.write(st_line + "\n")

                    pre_assets = parse_assets
                f.close()

    def run_every_people(self):
        with open(self.setting_position_filename) as f:
            ll = json.load(f)

            for setting in ll:
                self.people_figure_plot_people_name(setting["people"])
