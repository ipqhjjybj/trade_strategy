# coding=utf-8

from time import sleep
from tumbler.constant import Exchange
from tumbler.function import get_vt_key
from tumbler.record.client_quick_query import ClientPosPriceQuery
from tumbler.service import ding_talk_service, log_service_manager

okexs_contract_arr = ClientPosPriceQuery.query_exchange_info(Exchange.OKEXS.value)
huobiu_contract_arr = ClientPosPriceQuery.query_exchange_info(Exchange.HUOBIU.value)
huobi_contract_arr = [x.symbol for x in ClientPosPriceQuery.query_exchange_info(Exchange.HUOBI.value)]


def get_minus(f_ticker, s_ticker):
    a1 = s_ticker.bid_prices[0] / f_ticker.ask_prices[0] - 1
    a2 = f_ticker.bid_prices[0] / s_ticker.ask_prices[0] - 1
    return a1, a2


while True:
    contract_arr = okexs_contract_arr
    for contract in contract_arr:
        try:
            future_symbol = get_vt_key(contract.symbol, Exchange.OKEXS.value)
            spot_symbol = get_vt_key(contract.symbol.replace('usd_swap', 'usdt').replace('usdt_swap', 'usdt'),
                                     Exchange.OKEX.value)

            f_ticker = ClientPosPriceQuery.query_ticker(future_symbol)
            s_ticker = ClientPosPriceQuery.query_ticker(spot_symbol)

            sa, sb = get_minus(f_ticker, s_ticker)

            msg = "{} {} sa:{} sb:{}".format(future_symbol, spot_symbol, sa, sb)
            log_service_manager.write_log("[future_spot_alert] msg:{}".format(msg))
            if sa > 0.005 or sb > 0.005:
                ding_talk_service.send_msg(msg)
        except Exception as ex:
            log_service_manager.write_log("[Error] func1 ex:{}".format(ex))

    contract_arr = huobiu_contract_arr
    spot_contract_arr = huobi_contract_arr
    for contract in contract_arr:
        if contract.symbol in spot_contract_arr:
            try:
                future_symbol = get_vt_key(contract.symbol, Exchange.HUOBIU.value)
                spot_symbol = get_vt_key(contract.symbol, Exchange.HUOBI.value)

                f_ticker = ClientPosPriceQuery.query_ticker(future_symbol)
                s_ticker = ClientPosPriceQuery.query_ticker(spot_symbol)

                sa, sb = get_minus(f_ticker, s_ticker)

                msg = "{} {} sa:{} sb:{}".format(future_symbol, spot_symbol, sa, sb)
                log_service_manager.write_log("[future_spot_alert] msg:{}".format(msg))
                if sa > 0.005 or sb > 0.005:
                    ding_talk_service.send_msg(msg)
            except Exception as ex:
                log_service_manager.write_log("[Error] func2 ex:{}".format(ex))

    sleep(3)

# contract_arr = ClientPosPriceQuery.query_exchange_info(Exchange.HUOBIS.value)
# for contract in contract_arr:
#     future_symbol = get_vt_key(contract.symbol, Exchange.HUOBIS.value)
#     spot_symbol = get_vt_key(contract.symbol, Exchange.HUOBI.value)
#     #
#     f_ticker = ClientPosPriceQuery.query_ticker(future_symbol)
#     s_ticker = ClientPosPriceQuery.query_ticker(spot_symbol)
#     #
#     minus = (f_ticker.bid_prices[0] - s_ticker.bid_prices[0]) / s_ticker.bid_prices[0]
#     #
#     msg = "{} {} spread:{}".format(future_symbol, spot_symbol, minus)
#     log_service_manager.write_log("[future_spot_alert] msg:{}".format(msg))
#     if abs(minus) > 1.005:
#         ding_talk_service.send_msg(msg)


# okexs_contract_arr = ClientPosPriceQuery.query_exchange_info(Exchange.OKEXS.value)
# spot_contract_arr = [x.symbol for x in ClientPosPriceQuery.query_exchange_info(Exchange.HUOBI.value)]
# for contract in okexs_contract_arr:
#     c_symbol = contract.symbol.replace('usd_swap', 'usdt').replace('usdt_swap', 'usdt')
#     if c_symbol in spot_contract_arr:
#         future_symbol = get_vt_key(contract.symbol, Exchange.OKEXS.value)
#         spot_symbol = get_vt_key(c_symbol, Exchange.HUOBI.value)
#
#         f_ticker = ClientPosPriceQuery.query_ticker(future_symbol)
#         s_ticker = ClientPosPriceQuery.query_ticker(spot_symbol)
#
#         minus = (f_ticker.bid_prices[0] - s_ticker.bid_prices[0]) / s_ticker.bid_prices[0]
#
#         msg = "{} {} spread:{}".format(future_symbol, spot_symbol, minus)
#         log_service_manager.write_log("[future_spot_alert] msg:{}".format(msg))
#         if abs(minus) > 0.005:
#             ding_talk_service.send_msg(msg)

# okex_contract_arr = ClientPosPriceQuery.query_exchange_info(Exchange.OKEX.value)
# huobi_contract_arr = [x.symbol for x in ClientPosPriceQuery.query_exchange_info(Exchange.HUOBI.value)]
# for contract in okex_contract_arr:
#     c_symbol = contract.symbol
#     if c_symbol in huobi_contract_arr:
#         okex_spot_symbol = get_vt_key(contract.symbol, Exchange.OKEX.value)
#         huobi_spot_symbol = get_vt_key(c_symbol, Exchange.HUOBI.value)
#
#         f_ticker = ClientPosPriceQuery.query_ticker(okex_spot_symbol)
#         s_ticker = ClientPosPriceQuery.query_ticker(huobi_spot_symbol)
#
#         minus = (f_ticker.bid_prices[0] - s_ticker.bid_prices[0]) / s_ticker.bid_prices[0]
#
#         msg = "{} {} spread:{}".format(okex_spot_symbol, huobi_spot_symbol, minus)
#         log_service_manager.write_log("[future_spot_alert] msg:{}".format(msg))
#         if abs(minus) > 1.005:
#             ding_talk_service.send_msg(msg)
