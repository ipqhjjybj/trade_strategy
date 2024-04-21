# coding=utf-8

from tumbler.function import get_vt_key

APP_NAME = "DataThirdPart"

QUERY_ACCOUNT_EXCHANGE = "QUERY_ACCOUNT"
RECEIVE_ACCOUNT_EXCHANGE = "RECEIVE_ACCOUNT"


def get_query_account_name(account_name, exchange):
    """
    给query 账户查的账户
    :param account_name:
    :param exchange:
    :return:
    """
    if account_name:
        return get_vt_key(get_vt_key(QUERY_ACCOUNT_EXCHANGE, account_name), exchange)
    else:
        return QUERY_ACCOUNT_EXCHANGE


def get_diff_type_exchange_name(subscribe_type, vt_symbol="", account_name=""):
    """
    用于接收各种 外部信息的交换器的名字
    :param subscribe_type:
    :param vt_symbol:
    :param account_name:
    :return:
    """
    if vt_symbol:
        if account_name:
            return get_vt_key(subscribe_type, get_vt_key(account_name, vt_symbol))
        else:
            return get_vt_key(subscribe_type, vt_symbol)
    else:
        if account_name:
            return get_vt_key(subscribe_type, account_name)
        else:
            return subscribe_type


def get_receive_unique_queue(exchange_name, unique):
    return get_vt_key(exchange_name, unique)
