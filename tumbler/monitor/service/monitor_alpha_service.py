# coding=utf-8
import os

from tumbler.record.client_quick_query import ClientPosPriceQuery
from tumbler.service.log_service import log_service_manager
from tumbler.service.dingtalk_service import ding_talk_service
from tumbler.function import simple_load_json, get_from_vt_key
from tumbler.monitor.service.monitor_futures_service import MonitorFuturesService
from tumbler.object import ContractData


class MonitorAlphaBasicService(MonitorFuturesService):
    def __init__(self, settings):
        super(MonitorAlphaBasicService, self).__init__(settings)

    def run(self):
        for setting in self._settings:
            if not self.check_cta_runing(setting):
                name = setting.get("name", "unknown_name")
                symbol = setting.get("symbol")
                msg = "[MonitorAlphaService] {} cta not running! name:{}".format(symbol, name)
                log_service_manager.write_log(msg)
                ding_talk_service.send_msg(msg)
                return False

        return True


class MonitorAlphaService(MonitorFuturesService):
    def __init__(self, settings):
        super(MonitorAlphaService, self).__init__(settings)

    def run(self, now_day_long_coins=[], now_day_short_coins=[]):
        for setting in self._settings:
            if not self.check_run_pos_right(setting, now_day_long_coins, now_day_short_coins):
                name = setting.get("name", "unknown_name")
                msg = "[MonitorAlphaService] name:{} run false!".format(name)
                log_service_manager.write_log(msg)
                ding_talk_service.send_msg(msg)
            if not self.check_cta_runing(setting):
                name = setting.get("name", "unknown_name")
                symbol = setting.get("symbol")
                msg = "[MonitorAlphaService] {} cta not running! name:{}".format(symbol, name)
                log_service_manager.write_log(msg)
                ding_talk_service.send_msg(msg)
            self.check_error_in_important_file(setting)

    def check_run_pos_right(self, setting, now_day_long_coins=[], now_day_short_coins=[]):
        name = setting.get("name", "unknown_name")
        exchange = setting.get("exchange")
        api_key = setting.get("api_key", "")
        secret_key = setting.get("secret_key", "")
        passphrase = setting.get("passphrase", "")
        positions_path = setting.get("positions_path", "")
        max_amount = setting.get("max_amount", 0)

        contract_arr = ClientPosPriceQuery.query_exchange_info(exchange)
        contract_dic = ContractData.change_from_arr_to_dic(contract_arr)

        position_dic = simple_load_json(positions_path)
        exchange_position_list = ClientPosPriceQuery.query_position_info(exchange, api_key, secret_key, passphrase)
        for position in exchange_position_list:
            contract = contract_dic[position.vt_symbol]
            amount = position.position * position.price * contract.size
            if abs(amount) >= max_amount:
                msg = f"[MonitorAlphaService] name:{name}, {position.vt_symbol} {amount} exceed max_amount!!"
                log_service_manager.write_log(msg)
                ding_talk_service.send_msg(msg)
                return False

        if position_dic:
            for vt_symbol, pos in position_dic.items():
                symbol, exchange = get_from_vt_key(vt_symbol)

                if pos > 0 and symbol not in now_day_long_coins:
                    msg = f"[MonitorAlphaService] {symbol} not in now_day_long_coins!"
                    log_service_manager.write_log(msg)
                    ding_talk_service.send_msg(msg)
                    return False

                elif pos < 0 and symbol not in now_day_short_coins:
                    msg = f"[MonitorAlphaService] {symbol} not in now_day_short_coins!"
                    log_service_manager.write_log(msg)
                    ding_talk_service.send_msg(msg)
                    return False

        return True
