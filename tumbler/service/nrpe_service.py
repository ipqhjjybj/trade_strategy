# -*- coding=utf-8 -*-
# !/usr/bin/env python

import sys
import subprocess
import os
from datetime import datetime, timedelta

from tumbler.constant import NrpeState
from tumbler.function import get_last_line, get_datetime, get_last_several_lines
from .dingtalk_service import ding_talk_service

ErrorList = [
    "send_order request_error",
    "query_account_fund request_error"
]


class NrpeService(object):
    def __init__(self, _delay_minutes=5, _use_ding_talk: bool = False) -> None:
        self.delay_minutes = _delay_minutes
        self.use_ding_talk = _use_ding_talk

    def set(self, _delay_minutes: int = 5, _use_ding_talk: bool = False) -> None:
        self.delay_minutes = _delay_minutes
        self.use_ding_talk = _use_ding_talk

    def write_msg(self, msg: str) -> None:
        sys.stdout.write(msg)
        if self.use_ding_talk:
            ding_talk_service.send_msg(msg)

    def monitor_running(self, path: str, symbol: str, name: str) -> int:
        warn_msg = 'WARNING: [{}] symbol:{},path:{} not found!\n'.format(name, symbol, path)
        error_msg = 'CRITICAL: [{}] symbol:{},path:{} not running!\n'.format(name, symbol, path)
        last_line = get_last_line(path)
        if last_line is None:
            self.write_msg(warn_msg)
            return NrpeState.STATE_WARNING.value
        inside_datetime = get_datetime(last_line, self.delay_minutes)
        if inside_datetime < (datetime.now() - timedelta(minutes=self.delay_minutes)):
            self.write_msg(error_msg)
            return NrpeState.STATE_CRITICAL.value
        return NrpeState.STATE_OK.value

    def get_dealer_datetime(self, msg, delay_minutes):
        try:
            time_msg = msg.split(' ')[0]
            time_msg = time_msg.split('"')[1]
            time_msg = time_msg.split('+')[0]
            now_datetime = datetime.strptime(time_msg, "%Y-%m-%dT%H:%M:%S")
            return now_datetime
        except Exception as ex:
            self.write_msg("[get_dealer_datetime] ex:{}".format(ex))
            return datetime.now() - timedelta(minutes=delay_minutes)

    def monitor_dealer_running(self, path: str, warn_msg: str, error_msg: str) -> int:
        last_line = get_last_line(path)
        if last_line is None:
            self.write_msg(warn_msg)
            return NrpeState.STATE_WARNING.value
        inside_datetime = self.get_dealer_datetime(last_line, self.delay_minutes)
        if inside_datetime < (datetime.now() - timedelta(minutes=self.delay_minutes)):
            self.write_msg("inside_datetime:{} delay_minutes:{}"
                           .format(inside_datetime, datetime.now() - timedelta(minutes=self.delay_minutes)))
            self.write_msg(error_msg)
            return NrpeState.STATE_CRITICAL.value
        return NrpeState.STATE_OK.value

    def monitor_dealer_logs_dir(self, logs_dir: str) -> int:
        all_files = os.listdir(logs_dir)
        all_files.sort()
        if len(all_files) > 0:
            warn_msg = 'WARNING: [monitor_dealer_logs_dir] logs_dir:{} not found!\n'.format(logs_dir)
            error_msg = 'CRITICAL: [monitor_dealer_logs_dir] logs_dir:{}, not running!\n'.format(logs_dir)

            path = os.path.join(logs_dir, all_files[-1])
            return self.monitor_dealer_running(path, warn_msg, error_msg)
        else:
            warn_msg = 'WARNING: [monitor_dealer_logs_dir] logs_dir:{} not found!\n'.format(logs_dir)
            self.write_msg(warn_msg)
            return NrpeState.STATE_CRITICAL.value

    def monitor_future_spot_spread_running(self, symbol: str, path: str) ->int:
        return self.monitor_running(path, symbol, "monitor_future_spot_spread")

    def monitor_cta_running(self, symbol: str, path: str) ->int:
        return self.monitor_running(path, symbol, "monitor_cta_running")

    def monitor_volume_running(self, symbol: str, path: str) ->int:
        return self.monitor_running(path, symbol, "monitor_volume_running")

    def monitor_flash_running(self, symbol: str, path: str) -> int:
        return self.monitor_running(path, symbol, "monitor_flash_running")

    def monitor_market_maker_running(self, symbol: str, path: str) -> int:
        return self.monitor_running(path, symbol, "monitor_market_maker_running")

    def monitor_run_error(self, path: str, warn_msg: str, error_msg: str, unique_str: str = "") -> int:
        last_lines = get_last_several_lines(path)
        if not last_lines:
            self.write_msg(warn_msg + ",[unique]:{}".format(unique_str))
            return NrpeState.STATE_WARNING.value
        for line in ErrorList:
            for to_line in last_lines:
                if line in to_line:
                    self.write_msg(error_msg + "," + line + ",[unique]:{}".format(unique_str))
                    return NrpeState.STATE_CRITICAL.value
        return NrpeState.STATE_OK.value

    def check_log_file_old(self, filename: str, bef_days=2):
        if ".log" in filename and not filename.endswith(".log"):
            try:
                number = filename.split("log")[-1][1:]
                dt = datetime.strptime(number, '%Y%m%d%H%M%S')
                now = datetime.now()
                bef = now - timedelta(days=bef_days)
                if bef > dt:
                    return True
            except Exception:
                pass
        return False

    def restart_program(self, symbol: str, n_dir: str) -> None:
        print("now go to restart:{}".format(symbol))
        print(n_dir)
        cmd = "cd {} && bash restart.sh".format(n_dir)
        subprocess.Popen(cmd, shell=True)
        print("now go to end:{}".format(symbol))

    def start_dealer_v3(self, shell_name: str, n_dir: str) -> None:
        print("now go to start:{} dir:{}".format(shell_name, n_dir))
        cmd = "cd {} && bash {}".format(n_dir, shell_name)
        subprocess.Popen(cmd, shell=True)

    def start_dealer_v2(self, shell_name: str, n_dir: str) -> None:
        print("now go to start:{} dir:{}".format(shell_name, n_dir))
        cmd = "cd {} && bash {}".format(n_dir, shell_name)
        subprocess.Popen(cmd, shell=True)

    def clear_zip_log(self, n_dir: str) -> None:
        print("now go to dir:{}".format(n_dir))
        cmd = "cd {} && rm *.zip".format(n_dir)
        subprocess.Popen(cmd, shell=True)
        print("end clean dir:{}".format(n_dir))


nrpe_service = NrpeService()
