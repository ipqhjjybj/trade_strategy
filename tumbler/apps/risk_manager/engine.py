# coding=utf-8
from collections import defaultdict

from tumbler.engine import BaseEngine
from tumbler.object import OrderRequest
from tumbler.function import load_json
from tumbler.constant import Status
from tumbler.event import EVENT_TRADE, EVENT_ORDER, EVENT_TIMER
from tumbler.service import log_service_manager
from tumbler.event import Event

APP_NAME = "RiskManager"


class RiskManagerEngine(BaseEngine):
    setting_filename = "risk_manager_setting.json"

    def __init__(self, main_engine, event_engine):
        super().__init__(main_engine, event_engine, APP_NAME)
        self._send_order = None
        self.active = False

        self.order_size_limit = 100         # 订单最大下单数量限制

        self.order_flow_timer = 0           # 计时器, 每秒计数加1
        self.order_flow_clear = 60          # 每多少秒，数值清空

        self.order_flow_count = 0           # 订单流计数，包括挂单撤单等
        self.order_flow_limit = 50

        self.trade_count = 0
        self.trade_limit = 1000

        self.order_cancel_counts = defaultdict(int)
        self.order_cancel_limit = 500

        self.active_order_limit = 50

        self.load_setting()
        self.register_event()
        self.patch_send_order()

    def patch_send_order(self):
        """
        Patch send order function of MainEngine.
        """
        self._send_order = self.main_engine.send_order
        self.main_engine.send_order = self.send_order

    def send_order(self, req: OrderRequest, gateway_name: str):
        result = self.check_risk(req, gateway_name)
        if not result:
            return ""
        return self._send_order(req, gateway_name)

    def load_setting(self):
        setting = load_json(self.setting_filename)
        if not setting:
            return

        self.update_setting(setting)

    def update_setting(self, setting: dict):
        self.active = setting["active"]
        self.order_size_limit = setting["order_size_limit"]
        self.order_flow_clear = setting["order_flow_clear"]
        self.order_flow_limit = setting["order_flow_limit"]
        self.order_cancel_limit = setting["order_cancel_limit"]
        self.trade_limit = setting["trade_limit"]
        self.active_order_limit = setting["active_order_limit"]

        if self.active:
            log_service_manager.write_log("risk module start!")
        else:
            log_service_manager.write_log("risk module end!")

    def register_event(self):
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)

    def process_timer_event(self, event: Event):
        self.order_flow_timer += 1

        if self.order_flow_timer >= self.order_flow_clear:
            log_service_manager.write_log("[risk manager] order_flow_count:{} trade_count:{}"
                                          .format(self.order_flow_count, self.trade_count))
            for symbol, int_times in self.order_cancel_counts.items():
                log_service_manager.write_log("[risk manager] symbol:{} order_cancel_count:{}"
                                              .format(symbol, int_times))
            self.order_flow_count = 0
            self.trade_count = 0
            self.order_cancel_counts.clear()

            self.order_flow_timer = 0

    def process_order_event(self, event: Event):
        order = event.data
        if order.status != Status.CANCELLED.value:
            return
        self.order_cancel_counts[order.symbol] += 1

    def process_trade_event(self, event: Event):
        trade = event.data
        self.trade_count += trade.volume

    def check_risk(self, req: OrderRequest, gateway_name: str):
        if not self.active:
            return True

        # Check order volume
        if req.volume <= 0:
            log_service_manager.write_log("order volume must exceed 0")
            return False

        if req.volume > self.order_size_limit:
            log_service_manager.write_log("req volume:{}，exceed limit {}".format(req.volume, self.order_size_limit))
            return False

        # Check trade volume
        if self.trade_count >= self.trade_limit:
            log_service_manager.write_log("today_trade_num{}，exceed limit:{}".format(self.trade_count, self.trade_limit))
            return False

        # Check flow count
        if self.order_flow_count >= self.order_flow_limit:
            log_service_manager.write_log(
                "order flow volume {}，exceed per {} {}".format(
                    self.order_flow_count, self.order_flow_clear, self.order_flow_limit))
            return False

        # Check order cancel counts
        if req.symbol in self.order_cancel_counts and self.order_cancel_counts[req.symbol] >= self.order_cancel_limit:
            log_service_manager.write_log(
                "symbol:{} cancel times:{}，exceed limit{}".format(
                    req.symbol, self.order_cancel_counts[req.symbol], self.order_cancel_limit))
            return False

        # Check all active orders
        active_order_count = len(self.main_engine.get_all_active_orders())
        if active_order_count >= self.active_order_limit:
            log_service_manager.write_log(
                "now active order count {}，exceed limit{}".format(active_order_count, self.active_order_limit))
            return False

        # Add flow count if pass all checks
        self.order_flow_count += 1
        return True


