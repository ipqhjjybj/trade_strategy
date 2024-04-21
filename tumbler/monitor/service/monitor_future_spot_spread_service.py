# coding=utf-8
import os

from tumbler.record.client_quick_query import ClientPosPriceQuery
from tumbler.service.log_service import log_service_manager
from tumbler.service.dingtalk_service import ding_talk_service
from tumbler.service.nrpe_service import nrpe_service
from tumbler.service.nrpe_service import NrpeState
from tumbler.function import get_last_line


class MonitorFutureSpotSpreadService(object):
    """
    从文件下面
    策略文件日志中有这个值
    self.write_log("target_pos:{}, now pos:{}".format(self.target_pos, self.pos))

    settings:
    [
        {
            "name":"huobis_main_account",
            "symbol":,
            "exchange":,
            "api_key": "",
            "secret_key": "",
            "run_log_path":
        }
    ]
    """

    def __init__(self, settings):
        self._settings = settings
        self.run()

    def run(self):
        for setting in self._settings:
            if not self.check_future_spot_spread_running(setting):
                name = setting.get("name", "unknown_name")
                symbol = setting.get("symbol")
                msg = "[MonitorFuturesService] {} cta not running! name:{}".format(symbol, name)
                log_service_manager.write_log(msg)
                ding_talk_service.send_msg(msg)
            self.check_error_in_important_file(setting)

    def check_error_in_important_file(self, setting):
        name = setting.get("name", "unknown_name")
        imp_path = setting.get("important_file_path", "")
        if imp_path:
            last_line = get_last_line(imp_path)
            if last_line and "error" in last_line:
                msg = "[MonitorFuturesService] cta important error: {} {}!".format(name, last_line)
                log_service_manager.write_log(msg)
                ding_talk_service.send_msg(msg)

    def check_future_spot_spread_running(self, setting):
        symbol = setting.get("symbol")
        path = setting.get("run_log_path", "")
        if path:
            state = nrpe_service.monitor_future_spot_spread_running(symbol, path)
            if state != NrpeState.STATE_OK.value:
                return False
        return True
