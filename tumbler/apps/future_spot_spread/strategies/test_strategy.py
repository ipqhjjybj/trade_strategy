import time
from copy import copy

from tumbler.constant import TradeOrderSendType
from tumbler.apps.future_spot_spread.template import (
    FutureSpotSpreadTemplate
)

from tumbler.constant import MAX_PRICE_NUM, Exchange, DiffTypeFuture
from tumbler.object import ContractData
from tumbler.function import get_vt_key, get_round_order_price
from tumbler.constant import Direction, Status, Offset
from tumbler.function import get_two_currency
import tumbler.function.risk as risk
from tumbler.function.future_manager import FutureContractManager
from tumbler.apps.cta_strategy.template import NewOrderSendModule

'''
需要做的事，交易近一周合约与现货合约的差价
交易以吃单为主

1. 选择确定最近一周的合约是哪个， 次周合约是哪个
2、每周五固定时间 换期合约

3、判断当前是否处于系统可交易的时间
4、监控当前交易合约 与 现货的价差
5、发现价差达到交易范围
6、判断仓位资金是否充足，以及是否超过最大系统要求持仓 
7、开始吃单买入
8、价差回归则开始平仓
'''


def get_account_key_from_base_symbol(symbol):
    """
    btc_usd --> btc
    btc_usdt --> usdt
    """
    if symbol.endswith("_usd"):
        return symbol.replace("_usd", "")
    else:
        return "usdt"


class TestStrategy(FutureSpotSpreadTemplate):
    '''
    该策略只交易 this_week 品种的策略
    {
        "strategy_name": "",
        "class_name": "FutureSpotSpreadV1Strategy",
        "author": "ipqhjjybj",
        "spot_exchange": "HUOBI",
        "spot_symbol: "btc_usdt",
        "contract_exchange": "HUOBIF",
        "contract_base_symbol": "btc_usd",
        "fixed": 1,         # 固定合约数量大小
        "pos":  0,          # 当前仓位数量
        "long_spread_position_arr": [
            [0.6, -1],
            [1.2, -2],
        ],
        "long_cover_rate:":0,
        "short_spread_position_arr": [
            [-0.6, 1],
            [-1.2, 2],
        ],
        "short_cover_rate": 0,
        "leverage": 3
    }
    '''
    author = "ipqhjjybj"
    class_name = "TestStrategy"

    spot_symbol = "btc_usdt"
    contract_base_symbol = "btc_usd"

    spot_exchange = Exchange.OKEX.value
    contract_exchange = Exchange.OKEX5.value

    pos = 0  # 合约应该交易的数量
    target_pos = 0  # 目标应该下到的数量

    leverage = 3

    long_spread_position_arr = []
    short_spread_position_arr = []

    # 参数列表
    parameters = [
        'strategy_name',  #
        'class_name',  # 类的名字
        'author',  # 作者
        'spot_exchange',  # 现货交易所
        'contract_exchange',  # 期货交易所
        'spot_symbol',  # 现货币种
        'contract_base_symbol',  # 期货品种, btc_usd 以及 btc_usdt
        'fixed',  # 固定合约数量大小
        'pos',  # 当前仓位数量
        'long_spread_position_arr',  # 正价差，对应价差 && 下单对应仓位倍数
        'long_cover_rate',  # 平多点位
        'short_spread_position_arr',  # 负价差，对应价差 && 下单对应仓位倍数
        'short_cover_rate',  # 平空点位
    ]

    def __init__(self, cs_engine, strategy_name, settings):
        super(TestStrategy, self).__init__(cs_engine, strategy_name, settings)

        self.spot_tick_decorder = risk.TickDecorder(get_vt_key(self.spot_symbol, self.spot_exchange), self)
        self.contract_tick_dict = {}

        self.now_this_week_symbol = ""

        self.future_manager = FutureContractManager(self.contract_exchange, self)
        self.future_manager.run_update()

        self.order_module_dict = {}

        self.time_update_future_manager = risk.TimeWork(120)
        self.time_work = risk.TimeWork(1)

        target_symbol, base_symbol = get_two_currency(self.spot_symbol)
        self.spot_exchange_info = {
            "target_symbol": target_symbol,
            "base_symbol": base_symbol
        }
        self.contract_exchange_info = {
            "account_key": get_vt_key(self.contract_exchange,
                                      get_account_key_from_base_symbol(self.contract_base_symbol))
        }

        self.now_contract = None
        self.now_spread = None

    def on_init(self):
        self.write_log("[on_init]")

        contract = ContractData()
        contract.symbol = self.spot_symbol
        contract.exchange = self.spot_exchange
        contract.vt_symbol = get_vt_key(self.spot_symbol, self.spot_exchange)
        self.write_log(f"[on_init] go to subscribe contract {contract.vt_symbol}!")
        self.subscribe(contract)

    def on_start(self):
        self.write_log("[on_start]")
        self.update_contracts()
        self.update_now_contract()

    def on_stop(self):
        self.write_log("[on_stop]")

    def update_account(self):
        self.write_log("[update_account]")

        target_key = get_vt_key(self.spot_exchange, self.spot_exchange_info["target_symbol"])
        base_key = get_vt_key(self.spot_exchange, self.spot_exchange_info["base_symbol"])

        acct_te_target = self.get_account(target_key)
        acct_te_base = self.get_account(base_key)

        if acct_te_target is not None:
            self.spot_exchange_info["pos_target_symbol"] = acct_te_target.available
        if acct_te_base is not None:
            self.spot_exchange_info["pos_base_symbol"] = acct_te_base.available

        acct_te_contract = self.get_account(self.contract_exchange_info["account_key"])
        if acct_te_contract is not None:
            self.contract_exchange_info["account_val"] = acct_te_contract.balance

        contract_dict = self.future_manager.get_contract_dic_from_base_symbol(self.contract_base_symbol)
        for contract_type, contract_symbol in contract_dict.items():
            long_positon_key = get_vt_key(get_vt_key(contract_symbol, self.contract_exchange), Direction.LONG.value)
            short_position_key = get_vt_key(get_vt_key(contract_symbol, self.contract_exchange), Direction.SHORT.value)
            long_position = self.get_position(long_positon_key)
            add_pos = 0
            if long_position:
                add_pos += long_position.position

            short_position = self.get_position(short_position_key)
            if short_position:
                add_pos += short_position.position

            self.contract_exchange_info["pos_" + contract_symbol] = add_pos

        self.write_log("[update_account] spot_exchange_info:{} contract_exchange_info:{}"
                       .format(self.spot_exchange_info, self.contract_exchange_info))

    def check_update_account_ok(self):
        return "pos_target_symbol" in self.spot_exchange_info.keys()\
               and "account_val" in self.contract_exchange_info.keys()

    def update_now_contract(self):
        self.write_log("[update_now_contract]")
        self.now_contract = self.future_manager.get_now_contract_from_contract_type \
            (self.contract_base_symbol, DiffTypeFuture.THIS_WEEK.value)

    def compute_spread(self):
        self.write_log("[compute_spread]")

        contract_tick_decorder = self.contract_tick_dict[self.now_contract.vt_symbol]
        spot_tick_decorder = self.spot_tick_decorder

        if contract_tick_decorder.is_tick_ok() and spot_tick_decorder.is_tick_ok():
            self.now_spread = (contract_tick_decorder.tick.ask_prices[0] /
                               spot_tick_decorder.tick.bid_prices[0] - 1) * 100
            self.write_log("[compute_spread] now_spread:{}".format(self.now_spread))
            return True
        else:
            return False

    def compute_target_positions(self):
        self.write_log("[compute_target_positions]")

        if self.now_spread is not None:
            if not self.order_module_dict[self.now_contract.vt_symbol].is_trade_finished():
                self.write_log(f"[compute_target_positions] {self.now_contract.vt_symbol} trade not finished!")
                return
            if not self.order_module_dict[self.spot_tick_decorder.vt_symbol].is_trade_finished():
                self.write_log(f"[compute_target_positions] {self.spot_tick_decorder.vt_symbol} trade not finished!")
                return

            now_pos = self.order_module_dict[self.now_contract.vt_symbol].get_now_pos()

            s = 0
            for i in range(len(self.short_spread_position_arr)):
                s = s + self.short_spread_position_arr[i][2]
                self.short_spread_position_arr[i].append(s)

            cover_pos = 0
            for i in range(len(self.short_spread_position_arr)):
                if now_pos >= self.short_spread_position_arr[i][3] and self.now_spread >= \
                        self.short_spread_position_arr[i][1]:
                    cover_pos += self.short_spread_position_arr[i][2]

                if self.now_spread < self.short_spread_position_arr[i][0] \
                        and now_pos < self.short_spread_position_arr[i][3]:
                    now_pos = self.short_spread_position_arr[i][3]
                    cover_pos = 0

            s = 0
            for i in range(len(self.long_spread_position_arr)):
                s = s + self.long_spread_position_arr[i][2]
                self.long_spread_position_arr[i].append(s)

            for i in range(len(self.long_spread_position_arr)):
                if now_pos <= self.long_spread_position_arr[i][3] \
                        and self.now_spread <= self.long_spread_position_arr[i][1]:
                    cover_pos += self.long_spread_position_arr[i][2]

                if self.now_spread > self.long_spread_position_arr[i][0] and now_pos > self.long_spread_position_arr[i][3]:
                    now_pos = self.long_spread_position_arr[i][3]
                    cover_pos = 0

            self.target_pos = now_pos - cover_pos
            # for spread, vol in self.long_spread_position_arr:
            #     if self.now_spread > spread:
            #         self.target_pos += vol
            #
            # for spread, vol in self.short_spread_position_arr:
            #     if self.now_spread < spread:
            #         self.target_pos += vol

    def go_trade(self):
        if self.now_contract.vt_symbol in self.order_module_dict.keys():
            self.pos = self.order_module_dict[self.now_contract.vt_symbol].get_now_pos()
            now_target_pos = self.order_module_dict[self.now_contract.vt_symbol].get_target_pos()
            if self.target_pos != now_target_pos:
                self.write_log("[go_trade] self.target_pos:{} self.pos:{} now_target_pos:{}"
                               .format(self.target_pos, self.pos, now_target_pos))

                contract_pos = self.target_pos - now_target_pos
                self.pair_trade(self.now_contract.vt_symbol, contract_pos)

    def update_contracts(self):
        # 更新现货
        spot_contract = self.get_contract(get_vt_key(self.spot_symbol, self.spot_exchange))
        if spot_contract is not None:
            if spot_contract.vt_symbol not in self.order_module_dict.keys():
                self.order_module_dict[spot_contract.vt_symbol] =\
                    NewOrderSendModule.init_order_send_module_from_contract(
                        spot_contract, self, 0, wait_seconds=10, send_order_type=TradeOrderSendType.MARKET.value)
                self.order_module_dict[spot_contract.vt_symbol].start()

        # 更新合约
        self.future_manager.run_update()
        # 获得当前当周品种，次周品种
        # 如果当周品种发生更换或者当周合约快要到期
        #     -1. 如果该品种已经换季，清空该品种订单模块
        #     0. 初始化该品种订单管理模块
        #     1. 订阅最新的当周合约
        dic = self.future_manager.get_contract_dic_from_base_symbol(self.contract_base_symbol)
        if dic:
            if dic[DiffTypeFuture.THIS_WEEK.value] != self.now_this_week_symbol:
                self.write_log(f"[update_contracts] dic:{dic}")
                if self.now_this_week_symbol:
                    self.write_log(f"[update_contracts] go to clear {self.now_this_week_symbol}!")
                    vt_symbol = get_vt_key(self.now_this_week_symbol, self.contract_exchange)
                    if vt_symbol in self.order_module_dict.keys():
                        self.write_log(f"[update_contracts] pop {vt_symbol}!")
                        self.order_module_dict.pop(vt_symbol)

                    contract = self.future_manager.get_contract(
                        get_vt_key(self.now_this_week_symbol, self.contract_exchange))
                    if contract:
                        self.write_log(f"[update_contracts] now go to unsubscribe {self.now_this_week_symbol}! "
                                       f"contract:{contract.__dict__}")
                        self.unsubscribe(contract)
                    else:
                        self.write_log(f"[update_contracts] now go to unsubscribe contract is None! "
                                       f"{self.now_this_week_symbol}!")

                self.now_this_week_symbol = dic[DiffTypeFuture.THIS_WEEK.value]
                self.write_log(f"[update_contracts] now go to subscribe {self.now_this_week_symbol}!")

                contract = self.future_manager.get_contract(
                    get_vt_key(self.now_this_week_symbol, self.contract_exchange))
                self.subscribe(contract)

                self.order_module_dict[contract.vt_symbol] \
                    = NewOrderSendModule.init_order_send_module_from_contract(
                    contract, self, 0, wait_seconds=10, send_order_type=TradeOrderSendType.MARKET.value)
                self.order_module_dict[contract.vt_symbol].start()

                self.contract_tick_dict[get_vt_key(self.now_this_week_symbol, self.contract_exchange)] \
                    = risk.TickDecorder(get_vt_key(self.now_this_week_symbol, self.contract_exchange), self)
            elif not self.contract_tick_dict[get_vt_key(self.now_this_week_symbol, self.contract_exchange)] \
                    .has_not_inited():
                self.write_log(f"[update_contracts] tick has not updated! subscribe {self.now_this_week_symbol} again!")
                self.subscribe(self.future_manager.get_contract(get_vt_key(
                    self.now_this_week_symbol, self.contract_exchange)))
        else:
            self.write_log(f"[update_contracts] dic:{dic} updated failed!")

    def check_pos_ok(self, spot_need_trade_volume, contract_pos):
        flag = True
        if spot_need_trade_volume > 0:
            if self.spot_exchange_info["pos_target_symbol"] > spot_need_trade_volume:
                self.write_log("[check_pos_ok] spot_need_trade_volume:{} ok!".format(spot_need_trade_volume))
            else:
                flag = False
                self.write_log("[check_pos_ok] spot_need_trade_volume:{} not ok!".format(spot_need_trade_volume))
        else:
            if self.spot_exchange_info["pos_base_symbol"] > abs(spot_need_trade_volume) / \
                    self.spot_tick_decorder.tick.bid_prices[0]:
                self.write_log("[check_pos_ok] spot_need_trade_volume:{} ok!".format(spot_need_trade_volume))
            else:
                flag = False
                self.write_log("[check_pos_ok] spot_need_trade_volume:{} not ok!".format(spot_need_trade_volume))

        if abs(contract_pos) > 0:
            self.write_log("[check_pos_ok] contract_exchange_info account_val:{} "
                           .format(self.contract_exchange_info["account_val"]))
            if self.contract_exchange_info["account_val"] * self.spot_tick_decorder.tick.bid_prices[
                0] * self.leverage * 0.9 > self.now_contract.size * (
                    abs(contract_pos) + abs(self.contract_exchange_info["pos_" + self.now_contract.symbol])):
                self.write_log("[check_pos_ok] contract_pos:{} ok!".format(contract_pos))
            else:
                flag = False
                self.write_log("[check_pos_ok] contract_pos:{} not ok!".format(contract_pos))

        self.write_log("[check_pos_ok] end flag:{}".format(flag))
        return flag

    def pair_trade(self, contract_vt_symbol, contract_pos):
        if not self.trading:
            self.write_log(f"[pair_trade] not in trading condition. return!")
            return

        self.write_log(f"[pair_trade] contract_vt_symbol:{contract_vt_symbol}, contract_pos:{contract_pos}")
        contract = self.future_manager.get_contract(contract_vt_symbol)

        if contract:
            if contract.is_reverse():
                spot_need_trade_volume = contract.get_contract_val(contract_pos) \
                                         / self.spot_tick_decorder.tick.ask_prices[0]
            else:
                spot_need_trade_volume = contract.get_contract_val(contract_pos)

            spot_need_trade_volume = spot_need_trade_volume * -1
            self.write_log(f"[pair_trade] "
                           f"spot_vt_symbol:{self.spot_tick_decorder.vt_symbol}"
                           f"contract_vt_symbol:{contract_vt_symbol}"
                           f"go trade spot_pos:{spot_need_trade_volume} "
                           f"contract_pos:{contract_pos}")

            if self.check_pos_ok(spot_need_trade_volume, contract_pos):
                self.write_log(f"[pair_trade] now go to trade spot {spot_need_trade_volume}, contract:{contract_pos}!")
                now_spot_pos = self.order_module_dict[self.spot_tick_decorder.vt_symbol].get_now_pos()
                now_contract_pos = self.order_module_dict[contract_vt_symbol].get_now_pos()

                if abs(now_spot_pos + self.now_contract.get_contract_val(now_contract_pos)) \
                        < self.now_contract.get_contract_val(1):
                    self.order_module_dict[self.spot_tick_decorder.vt_symbol].go_new_trade_pos(spot_need_trade_volume)
                    self.order_module_dict[contract_vt_symbol].go_new_trade_pos(contract_pos)

                    self.write_log(f"[pair_trade] spot_now_pos:{self.order_module_dict[self.spot_tick_decorder.vt_symbol].get_target_pos()}"
                                   f" contract_pos:{self.order_module_dict[contract_vt_symbol].get_target_pos()}")
                else:
                    self.write_log(f"[pair_trade] check pos not equal! so not go to trade! "
                                   f" now_spot_pos:{now_spot_pos}, now_contract_pos:{now_contract_pos}")
            else:
                msg = "[pair_trade] check pos not right! so not go to trade!"
                self.write_log(msg)
                self.send_ding_msg(msg)

    def clear_symbol(self, contract_vt_symbol):
        # 清空某个品种的仓位
        self.write_log(f"[clear_symbol] now go {contract_vt_symbol}")
        if contract_vt_symbol in self.order_module_dict.keys():
            order_module = self.order_module_dict[contract_vt_symbol]
            now_pos = order_module.get_now_pos()

            self.pair_trade(contract_vt_symbol, -1 * now_pos)

    def on_tick(self, tick):
        # self.write_log("[on_tick] vt_symbol:{} bid_price0:{} ask_prices0:{}"
        #                .format(tick.vt_symbol, tick.bid_prices[0], tick.ask_prices[0]))
        contract = self.get_contract(tick.vt_symbol)
        if contract.exchange != Exchange.HUOBI.value:
            self.unsubscribe(contract)

    def on_order(self, order):
        msg = f"[on_order] {order.vt_symbol}, {order.direction}, {order.price}, " \
              f"order.volume:{order.volume}, order.traded:{order.traded}"
        self.write_log(msg)

        if order.vt_symbol in self.order_module_dict.keys():
            self.order_module_dict[order.vt_symbol].on_order(copy(order))

        if not order.is_active() and order.traded > 0:
            self.write_important_log(msg)

    def on_trade(self, trade):
        self.write_log('[on_trade] start')
        msg = '[trade detail] :{}'.format(trade.__dict__)
        self.write_important_log(msg)
        self.send_ding_msg(msg)

    def output_condition(self):
        pass
