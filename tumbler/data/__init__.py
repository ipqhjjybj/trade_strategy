# coding=utf-8

from tumbler.constant import Exchange

from .binance_data import BinanceClient
from .okex5_data import Okex5Client

data_client_dict = {
    Exchange.OKEX5.value: Okex5Client,
    Exchange.BINANCE.value: BinanceClient,
}
