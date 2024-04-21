# coding=utf-8
import os

from tumbler.record.client_quick_query import ClientPosPriceQuery
from tumbler.service.log_service import log_service_manager
from tumbler.service.dingtalk_service import ding_talk_service
from tumbler.service.nrpe_service import nrpe_service
from tumbler.service.nrpe_service import NrpeState
from tumbler.function import get_last_line, simple_load_json, get_from_vt_key


class MonitorFuturesService(object):
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
            if not self.check_run_pos_right(setting):
                name = setting.get("name", "unknown_name")
                msg = "[MonitorFuturesService] name:{} run false!".format(name)
                log_service_manager.write_log(msg)
                ding_talk_service.send_msg(msg)
            if not self.check_cta_runing(setting):
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

    def check_cta_runing(self, setting):
        symbol = setting.get("symbol")
        path = setting.get("run_log_path", "")
        if path:
            state = nrpe_service.monitor_cta_running(symbol, path)
            if state != NrpeState.STATE_OK.value:
                return False
        return True

    def check_run_pos_right(self, setting):
        name = setting.get("name", "unknown_name")
        symbol = setting.get("symbol")
        exchange = setting.get("exchange")
        api_key = setting.get("api_key", "")
        secret_key = setting.get("secret_key", "")
        passphrase = setting.get("passphrase", "")
        max_pos = setting.get("max_pos", 1000)
        path = setting.get("run_log_path", "")

        pos = 0
        try:
            position_dic = ClientPosPriceQuery.query_position_info(exchange, api_key, secret_key, passphrase)
            log_service_manager.write_log("position_dic:{}".format(position_dic))
            pos = float(position_dic[symbol])
            if abs(pos) > float(max_pos) + 1e-8:
                log_service_manager.write_log("[check_run_pos_right] pos exceed max pos!")
                return False
        except Exception as ex:
            log_service_manager.write_log("[check_run_pos_right] fun1 name:{} ex:{}".format(name, ex))

        if path:
            if os.path.exists(path):
                last_pos_line = ""
                f = open(path, "r")
                for line in f:
                    if "target_pos" in line and "now pos" in line:
                        last_pos_line = line.strip()
                f.close()
                try:
                    if last_pos_line:
                        v = (last_pos_line[last_pos_line.index("target_pos"):]).split(', ')
                        target_pos, now_pos = [x.split(":")[1] for x in v]
                        log_service_manager.write_log("target_pos:{} now_pos:{}".format(target_pos, now_pos))

                        if abs(pos) > float(max_pos) + 1e-8:
                            log_service_manager.write_log("[check_run_pos_right] pos exceed max pos!")
                            return False
                        return abs(float(now_pos) - float(pos)) < 1e-8
                    else:
                        log_service_manager.write_log("[check_run_pos_right] last_pos_line empty!")
                except Exception as ex:
                    log_service_manager.write_log("[check_run_pos_right] name:{} ex:{}".format(name, ex))
            else:
                log_service_manager.write_log("[check_run_pos_right] unknown path:{}".format(name, path))

        return True
