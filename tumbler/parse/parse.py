# coding=utf-8

from copy import copy

from tumbler.constant import Exchange, MQSubscribeType
from tumbler.object import MQSubscribeRequest
from tumbler.function.function import get_two_currency, get_from_vt_key


def parse_get_all_symbols(maker_0x_setting):
    """
    从 0x_maker_setting.json中解析出
    """
    symbols = []
    for strategy_name in maker_0x_setting.keys():
        setting = maker_0x_setting[strategy_name].get("setting", {})
        symbol_pair = setting.get("symbol_pair", "")
        if symbol_pair:
            symbols.append(symbol_pair)
    return list(set(symbols))


def parse_get_all_vt_symbols(settings):
    all_vt_symbols = set([])
    for key_strategy in settings.keys():
        setting = settings[key_strategy]["setting"]
        vt_symbols_subscribe = setting.get("vt_symbols_subscribe", [])
        for vt_symbol in vt_symbols_subscribe:
            all_vt_symbols.add(vt_symbol)
    return all_vt_symbols


def parse_get_data_third_part_setting(market_maker_setting):
    ret_subs = []
    for strategy_name, setting in market_maker_setting.items():
        third_part = setting["third_part_load"]
        print(third_part)
        for info in third_part:
            tp = info["type"]
            if tp in [MQSubscribeType.BBO_TICKER.value]:
                for exchange in info["subscribes"]:
                    sub = MQSubscribeRequest()
                    sub.subscribe_type = tp
                    sub.symbol = ""
                    sub.exchange = exchange
                    sub.vt_symbol = sub.exchange
                    sub.unique = strategy_name
                    ret_subs.append(copy(sub))
            elif tp in [
                MQSubscribeType.TIKER.value,
                MQSubscribeType.MERGE_TICKER.value
            ]:
                for vt_symbol in info["subscribes"]:
                    symbol, exchange = get_from_vt_key(vt_symbol)

                    sub = MQSubscribeRequest()
                    sub.subscribe_type = tp
                    sub.symbol = symbol
                    sub.exchange = exchange
                    sub.vt_symbol = vt_symbol
                    sub.unique = strategy_name
                    ret_subs.append(copy(sub))

            elif tp in [MQSubscribeType.DICT_ACCOUNT.value,
                        MQSubscribeType.ACCOUNT.value]:
                for account_name in info["subscribes"]:
                    sub = MQSubscribeRequest()
                    sub.subscribe_type = tp
                    sub.account_name = account_name
                    sub.unique = strategy_name
                    ret_subs.append(copy(sub))

            elif tp in [MQSubscribeType.TRADE_DATA.value,
                        MQSubscribeType.SEND_ORDER.value,
                        MQSubscribeType.COVER_ORDER_REQUEST.value,
                        MQSubscribeType.REJECT_COVER_ORDER_REQUEST.value]:
                for key in info["subscribes"]:
                    sub = MQSubscribeRequest()
                    sub.subscribe_type = tp
                    sub.vt_symbol = key
                    sub.unique = strategy_name
                    ret_subs.append(copy(sub))

    return ret_subs


def parse_get_super_settings(market_maker_setting):
    """
    从 market_maker_strategy.json 中解析出 需要订阅的第三方数据
    """
    ticks_set = set([])
    for strategy_name in market_maker_setting.keys():
        setting = market_maker_setting[strategy_name].get("setting", {})
        for vt_symbol in setting["vt_symbols_subscribe"]:
            ticks_set.add(vt_symbol)
    return ticks_set


def parse_get_monitor_setting(market_maker_setting):
    """
    从 market_maker_seting.json 中解析出 需要监控的第三方数据
    """
    record_assets = []
    exchanges = []
    for strategy_name in market_maker_setting.keys():
        class_name = market_maker_setting[strategy_name].get("class_name", "")
        if class_name in ["MarketMakerV1Strategy", "MarketMakerTestMovStrategy"]:
            setting = market_maker_setting[strategy_name].get("setting", {})
            target_exchange_info = setting.get("target_exchange_info", {})
            target_exchange_name = target_exchange_info.get("exchange_name", "")

            base_exchange_info = setting.get("base_exchange_info", {})

            target_symbol = setting.get("target_symbol", "")
            base_symbol = setting.get("base_symbol", "")

            record_assets.append(target_symbol)
            record_assets.append(base_symbol)

            if len(target_exchange_name) > 0:
                exchanges.append(target_exchange_name)
            for exchange_name in list(base_exchange_info.keys()):
                exchanges.append(exchange_name)
        elif class_name == "GridMakerV1Strategy":
            setting = market_maker_setting[strategy_name].get("setting", {})
            target_symbol, base_symbol = get_two_currency(setting.get("symbol_pair", ""))

            record_assets.append(target_symbol)
            record_assets.append(base_symbol)

            exchange_info = setting.get("exchange_info", {})
            exchange_name = exchange_info["exchange_name"]

            exchanges.append(exchange_name)
        elif class_name == "Flash0xStrategy":
            setting = market_maker_setting[strategy_name].get("setting", {})
            target_exchange_info = setting.get("target_exchange_info", {})
            target_exchange_name = target_exchange_info.get("exchange_name", "")

            base_exchange_info = setting.get("base_exchange_info", {})

            target_symbol = setting.get("target_symbol", "")
            base_symbol = setting.get("base_symbol", "")

            record_assets.append(target_symbol)
            record_assets.append(base_symbol)

            if len(target_exchange_name) > 0:
                exchanges.append(target_exchange_name)
            for exchange_name in list(base_exchange_info.keys()):
                exchanges.append(exchange_name)

    record_assets = list(set(record_assets))
    exchanges = list(set(exchanges))
    return {"record_assets": record_assets, "exchanges": exchanges}


def parse_get_exchange_symbol_setting(market_maker_setting):
    """
    从 market_maker_seting.json 中解析出 exchange 与 symbol的关系映射
    """
    ret_dict = {}
    for strategy_name in market_maker_setting.keys():
        class_name = market_maker_setting[strategy_name].get("class_name", "")
        if class_name == "MarketMakerV1Strategy":
            setting = market_maker_setting[strategy_name].get("setting", {})
            symbol_pair = setting["symbol_pair"]
            target_exchange = setting["target_exchange_info"]["exchange_name"]
            base_exchange = setting["base_exchange_info"]["exchange_name"]
            if target_exchange not in ret_dict.keys():
                ret_dict[target_exchange] = []
            if base_exchange not in ret_dict.keys():
                ret_dict[base_exchange] = []

            ret_dict[target_exchange].append(symbol_pair)
            ret_dict[base_exchange].append(symbol_pair)

        elif class_name == "GridMakerV1Strategy":
            setting = market_maker_setting[strategy_name].get("setting", {})
            symbol_pair = setting["symbol_pair"]
            exchange = setting["exchange_info"]["exchange_name"]
            if exchange not in ret_dict.keys():
                ret_dict[exchange] = []
            ret_dict[exchange].append(symbol_pair)

        elif class_name == "FutureCrossV1Strategy":
            setting = market_maker_setting[strategy_name].get("setting", {})
            target_exchange_info = setting["target_exchange_info"]
            base_exchange_info = setting["base_exchange_info"]
            if target_exchange_info["exchange"] not in ret_dict.keys():
                ret_dict[target_exchange_info["exchange"]] = []
            if base_exchange_info["exchange"] not in ret_dict.keys():
                ret_dict[base_exchange_info["exchange"]] = []

            ret_dict[target_exchange_info["exchange"]].append(target_exchange_info["symbol"])
            ret_dict[base_exchange_info["exchange"]].append(base_exchange_info["symbol"])

    return ret_dict
