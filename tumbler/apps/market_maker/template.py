# coding=utf-8

import time
from copy import copy
from tumbler.object import TradeData, DictAccountData, CoverOrderRequest, AccountData, RejectCoverOrderRequest
from tumbler.constant import Direction, Offset, MQSubscribeType
from tumbler.apps.data_third_part.base import get_diff_type_exchange_name
from tumbler.function import FilePrint


class MarketMakerSignal(object):
    def on_tick(self, tick):
        pass

    def on_bar(self, bar):
        pass


class MarketMakerTemplate(object):
    author = ""
    class_name = "MarketMakerTemplate"
    target_symbol = "btc"
    base_symbol = "usdt"

    inited = False
    trading = False

    # bbo
    vt_bbo_subscribe = []
    # 订阅行情 
    vt_symbols_subscribe = []
    # 订阅账户数据
    vt_account_name_subscribe = []
    # 允许转账的资产
    vt_transfer_assets = []

    # 参数列表
    parameters = []

    # 运行时  重点变量列表
    variables = []

    def __init__(self, mm_engine, strategy_name, settings):
        # 设置策略的参数
        if settings:
            d = self.__dict__
            for key in self.parameters:
                if key in settings:
                    d[key] = copy(settings[key])

        self.mm_engine = mm_engine
        self.strategy_name = strategy_name

        self.variables = copy(self.variables)
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")

        self.file_print = FilePrint(self.strategy_name + ".log", "strategy_run_log", mode="w")
        self.important_print = FilePrint(self.strategy_name + "_important.log", "important_strategy_log", mode="w")

        self.working_transfer_request = None

    def write_important_log(self, msg):
        """
        Write important message
        """
        self.write_log(msg)
        self.important_print.write('[{}]:{}'.format(self.strategy_name, msg))

    def write_log(self, msg):
        """
        Write a log message.
        """
        self.file_print.write('[{}]:{}'.format(self.strategy_name, msg))

    def get_contract(self, vt_symbol):
        """
        获得合约信息 , 从 engine中
        """
        return self.mm_engine.get_contract(vt_symbol)

    def get_account(self, vt_account_id):
        """
        获得账户的信息 , 从 engine中
        """
        return self.mm_engine.get_account(vt_account_id)

    def batch_orders(self, reqs):
        """
        send batch orders to system
        """
        return []

    def cancel_orders(self, vt_order_ids):
        """
        cancel batch orders 
        """
        pass

    def buy(self, symbol, exchange, price, volume, stop=False, lock=False):
        """
        Send buy order to open a long position.
        """
        return self.send_order(symbol, exchange, Direction.LONG.value, Offset.OPEN.value, price, volume, stop, lock)

    def sell(self, symbol, exchange, price, volume, stop=False, lock=False):
        """
        Send sell order to close a long position.
        """
        return self.send_order(symbol, exchange, Direction.SHORT.value, Offset.CLOSE.value, price, volume, stop, lock)

    def short(self, symbol, exchange, price, volume, stop=False, lock=False):
        """
        Send short order to open as short position.
        """
        return self.send_order(symbol, exchange, Direction.SHORT.value, Offset.OPEN.value, price, volume, stop, lock)

    def cover(self, symbol, exchange, price, volume, stop=False, lock=False):
        """
        Send cover order to close a short position.
        """
        return self.send_order(symbol, exchange, Direction.LONG.value, Offset.CLOSE.value, price, volume, stop, lock)

    def send_order(self, symbol, exchange, direction, offset, price, volume, stop=False, lock=False):
        """
        Send a new order.
        """
        if self.trading:
            if stop:
                vt_order_ids = self.mm_engine.send_order(self, symbol, exchange, direction, offset, price, volume,
                                                         stop=True, lock=lock)
                msg = "[send_stop_order] vt_order_ids:{} info:{},{},{},{},{},{}".format(vt_order_ids, symbol,
                                                                                        exchange, direction, offset,
                                                                                        price, volume)
                self.write_log(msg)
                return vt_order_ids
            else:
                vt_order_ids = self.mm_engine.send_order(self, symbol, exchange, direction, offset, price, volume)
                msg = "[send_order] vt_order_ids:{} info:{},{},{},{},{},{}".format(vt_order_ids, symbol,
                                                                                   exchange, direction, offset, price,
                                                                                   volume)
                self.write_log(msg)
                return vt_order_ids
        else:
            msg = "[send_order] trading condition is false!"
            self.write_log(msg)
            self.write_important_log(msg)
            return [[None, None]]

    def send_mq_msg(self, exchange_name, msg):
        self.write_log("[send_mq_msg] trading:{} exchange_name:{} msg:{}".format(self.trading, exchange_name, msg))
        if self.trading:
            return self.mm_engine.send_mq_msg(exchange_name, msg)

    def send_cover_trades(self, trade: TradeData):
        exchange_name = get_diff_type_exchange_name(MQSubscribeType.TRADE_DATA.value)
        self.send_mq_msg(exchange_name, trade.get_mq_msg())

    def send_cover_order_req(self, req: CoverOrderRequest):
        exchange_name = get_diff_type_exchange_name(MQSubscribeType.COVER_ORDER_REQUEST.value, req.vt_symbol)
        self.send_mq_msg(exchange_name, req.get_mq_msg())

    def send_reject_cover_order_req(self, req: RejectCoverOrderRequest):
        exchange_name = get_diff_type_exchange_name(MQSubscribeType.REJECT_COVER_ORDER_REQUEST.value, req.vt_symbol)
        self.send_mq_msg(exchange_name, req.get_mq_msg())

    def send_cover_account(self, acct: AccountData):
        exchange_name = get_diff_type_exchange_name(MQSubscribeType.ACCOUNT.value)
        self.send_mq_msg(exchange_name, acct.get_mq_msg())

    def send_cover_account_dict(self, acct: DictAccountData):
        exchange_name = get_diff_type_exchange_name(MQSubscribeType.DICT_ACCOUNT.value, account_name=acct.account_name)
        self.send_mq_msg(exchange_name, acct.get_mq_msg())

    def send_put_trades(self, trade: TradeData):
        # self.write_log("[send_put_trades] trade:{}".format(trade.__dict__))
        exchange_name = get_diff_type_exchange_name(MQSubscribeType.TRADE_DATA.value, trade.vt_symbol)
        self.send_mq_msg(exchange_name, trade.get_mq_msg())

    def update_single_exchange_info(self, dic, contract_key):
        contract = self.get_contract(contract_key)
        if contract is None:
            update_msg = "target_contract:{} is not found!".format(contract_key)
            self.write_log(update_msg)
            return False

        dic["price_tick"] = contract.price_tick
        dic["volume_tick"] = contract.volume_tick
        dic["min_volume"] = contract.min_volume
        return True

    def get_make_cover_volume(self, dic):
        already_need_make_cover_target_volume = 0
        already_need_make_cover_base_volume = 0
        already_price_volumes = []
        for key, s_order in dic.items():
            volume = s_order.volume - s_order.traded
            already_price_volumes.append((s_order.price, volume))
            already_need_make_cover_target_volume += volume
            already_need_make_cover_base_volume += s_order.price * volume
        return already_need_make_cover_target_volume, already_need_make_cover_base_volume, already_price_volumes

    def update_order(self, order_dict, order, func=None):
        bef_order = order_dict.get(order.vt_order_id, None)
        if bef_order:
            new_traded = order.traded - bef_order.traded
            if new_traded > 0:
                order_dict[order.vt_order_id] = copy(order)
                if func:
                    func(order, new_traded)

            if not order.is_active():
                del order_dict[order.vt_order_id]
        return bef_order

    def on_transfer(self, transfer_req):
        """
        :param transfer_req:
        :return:
        """
        msg = "[process_transfer_event] :{}".format(transfer_req.__dict__)
        self.write_important_log(msg)

        if self.working_transfer_request:
            msg = "[process_transfer_event] already has transfer req! drop it!"
            self.write_important_log(msg)
            return

        if transfer_req.asset_id not in self.vt_transfer_assets:
            msg = "[process_transfer_event] asset_id:{} not existed".format(transfer_req.asset_id)
            self.write_important_log(msg)
            return

        self.working_transfer_request = copy(transfer_req)
        self.process_transfer()

    def has_prepare_transfer(self, req):
        return True

    def after_transfer_asset(self, req):
        pass

    def process_transfer(self):
        if not self.working_transfer_request:
            return

        if self.working_transfer_request:
            now = time.time()
            # 超过一分钟的转账请求，丢弃掉
            if now - self.working_transfer_request.timestamp > 60:
                msg = "[process_transfer] before drop working_transfer request for time exceed! a:{},b:{}". \
                    format(now, self.working_transfer_request.timestamp)
                self.write_important_log(msg)
                self.working_transfer_request = None
                return

        if self.working_transfer_request.asset_id in [self.target_symbol, self.base_symbol]:
            if self.has_prepare_transfer(self.working_transfer_request):
                finished_transfer_id = self.mm_engine.transfer_amount(self.working_transfer_request)
                msg = "[process_transfer] transfer_amount result:{}".format(finished_transfer_id)
                self.write_important_log(msg)

                if finished_transfer_id:
                    self.after_transfer_asset(self.working_transfer_request)
                    self.working_transfer_request = None
                else:
                    now = time.time()
                    if now - self.working_transfer_request.timestamp > 20:
                        msg = "[process_transfer] send drop working_transfer request for time exceed! a:{},b:{}".format(
                            now, self.working_transfer_request.timestamp)
                        self.write_important_log(msg)
                        self.working_transfer_request = None

    def on_tick(self, tick):
        """
        Callback of new tick data update.
        """
        if self.trading and self.working_transfer_request:
            self.process_transfer()

    def on_bbo_tick(self, tick):
        """
        Callback of new bbo_tick data update.
        """
        pass

    def cancel_order(self, vt_order_id):
        """
        Cancel an existing order.
        """
        msg = "[cancel order] vt_order_id:{}".format(vt_order_id)
        self.write_log(msg)
        self.mm_engine.cancel_order(self, vt_order_id)

    def get_parameters(self):
        """
        Get strategy parameters dict.
        """
        strategy_parameters = {}
        for name in self.parameters:
            strategy_parameters[name] = getattr(self, name)
        return strategy_parameters

    def get_variables(self):
        """
        Get strategy variables dict.
        """
        strategy_variables = {}
        for name in self.variables:
            strategy_variables[name] = getattr(self, name)
        return strategy_variables

    def get_data(self):
        """
        Get strategy data.
        """
        strategy_data = {
            "parameters": self.get_parameters(),
            "variables": self.get_variables(),
        }
        return strategy_data

    def put_event(self):
        """
        Put an strategy data event for ui update.
        """
        if self.inited:
            self.mm_engine.put_strategy_event(self)

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        pass

    def on_start(self):
        """
        Callback when strategy is started.
        """
        pass

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        pass

    def on_merge_tick(self, merge_tick):
        """
        Callback of new merge tick data update.
        """
        pass

    def on_trade(self, trade):
        """
        Callback of new trade data update.
        """
        pass

    def on_order(self, order):
        """
        Callback of new order data update.
        """
        pass

    def on_dict_account(self, dict_acct):
        """
        Call back of account transfer
        :param acct:
        :return:
        """
        pass

    def on_reject_cover_order_request(self, req):
        """
        :param req:
        :return:
        """
        pass

    def on_cover_order_request(self, req):
        """
        :param req:
        :return:
        """
        pass

