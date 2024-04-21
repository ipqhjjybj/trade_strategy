# coding=utf-8

from datetime import datetime
from tumbler.function import get_two_currency, utc_2_local

REST_MARKET_HOST = "https://bittrex.com"
REST_TRADE_HOST = "https://bittrex.com"


def _bittrex_format_symbol(symbol):
    target_symbol, base_symbol = get_two_currency(symbol)
    return ('{}-{}'.format(base_symbol, target_symbol)).upper()


def _bittrex_symbol_to_system_symbol(symbol):
    base_symbol, target_symbol = symbol.split('-')
    return ('{}_{}'.format(target_symbol, base_symbol)).lower()


def _bittrex_parse_datetime(st):
    return datetime.strptime(st, "%Y-%m-%dT%H:%M:%S.%f")


def _bittrex_to_system_format_date(st):
    if "." in st:
        dt = utc_2_local(datetime.strptime(st, "%Y-%m-%dT%H:%M:%S.%f"))
    else:
        dt = utc_2_local(datetime.strptime(st, "%Y-%m-%dT%H:%M:%S"))
    return dt.strftime("%Y-%m-%d %H:%M:%S")
