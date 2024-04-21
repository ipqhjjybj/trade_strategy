# coding=utf-8

from tumbler.constant import Exchange

from .binance import BinanceGateway, BinanceRestMarketApi, BinanceWsMarketApi, BinanceBBORestMarketApi
from .binancef import BinancefGateway, BinancefRestMarketApi, BinancefWsMarketApi, BinancefBBORestMarketApi
from .bitfinex import BitfinexGateway, BitfinexRestMarketApi, BitfinexWsMarketApi
from .bitmex import BitmexGateway
from .bittrex import BittrexGateway, BittrexRestMarketApi
from .coinex import CoinexGateway
from .coinexs import CoinexsGateway
from .gateio import GateioGateway, GateioRestMarketApi, GateioWsMarketApi
from .huobi import HuobiGateway, HuobiRestMarketApi, HuobiWsMarketApi
from .huobis import HuobisGateway, HuobisRestMarketApi, HuobisWsMarketApi
from .huobiu import HuobiuGateway, HuobiuRestMarketApi, HuobiuWsMarketApi, HuobiuBBORestMarketApi
from .huobif import HuobifGateway, HuobifRestMarketApi, HuobifWsMarketApi

from .nexus import NexusGateway, NexusRestMarketApi
from .okex import OkexGateway, OkexWsMarketApi, OkexRestMarketApi
from .okexs import OkexsGateway, OkexsRestMarketApi, OkexsWsMarketApi
from .okexf import OkexfGateway, OkexfRestMarketApi, OkexfWsMarketApi
from .okex5 import Okex5Gateway, Okex5RestMarketApi, Okex5WsMarketApi, Okex5BBORestMarketApi


gateway_dict = {
    Exchange.BINANCE.value: BinanceGateway,
    Exchange.BINANCEF.value: BinancefGateway,
    Exchange.BITFINEX.value: BitfinexGateway,
    Exchange.BITMEX.value: BitmexGateway,
    Exchange.BITTREX.value: BittrexGateway,
    Exchange.COINEX.value: CoinexGateway,
    Exchange.COINEXS.value: CoinexsGateway,
    Exchange.GATEIO.value: GateioGateway,
    Exchange.HUOBI.value: HuobiGateway,
    Exchange.HUOBIU.value: HuobiuGateway,
    Exchange.HUOBIS.value: HuobisGateway,
    Exchange.HUOBIF.value: HuobifGateway,
    Exchange.NEXUS.value: NexusGateway,
    Exchange.OKEX.value: OkexGateway,
    Exchange.OKEXS.value: OkexsGateway,
    Exchange.OKEXF.value: OkexfGateway,
    Exchange.OKEX5.value: Okex5Gateway
}

bbo_exchange_map = {
    Exchange.HUOBIU.value: HuobiuBBORestMarketApi,
    Exchange.BINANCE.value: BinanceBBORestMarketApi,
    Exchange.BINANCEF.value: BinancefBBORestMarketApi,
    Exchange.OKEX5.value: Okex5BBORestMarketApi
}

rest_exchange_map = {
    Exchange.OKEX.value: OkexRestMarketApi,
    Exchange.OKEXS.value: OkexsRestMarketApi,
    Exchange.OKEXF.value: OkexfRestMarketApi,
    Exchange.HUOBI.value: HuobiRestMarketApi,
    Exchange.HUOBIU.value: HuobiuRestMarketApi,
    Exchange.HUOBIS.value: HuobisRestMarketApi,
    Exchange.HUOBIF.value: HuobifRestMarketApi,
    Exchange.GATEIO.value: GateioRestMarketApi,
    Exchange.BINANCE.value: BinanceRestMarketApi,
    Exchange.BITFINEX.value: BitfinexRestMarketApi,
    Exchange.BITTREX.value: BittrexRestMarketApi,
    Exchange.NEXUS.value: NexusRestMarketApi,
    Exchange.BINANCEF.value: BinancefRestMarketApi,
    Exchange.OKEX5.value: Okex5BBORestMarketApi
}

ws_exchange_map = {
    Exchange.OKEX.value: OkexWsMarketApi,
    Exchange.OKEXS.value: OkexsWsMarketApi,
    Exchange.OKEXF.value: OkexfWsMarketApi,
    Exchange.HUOBI.value: HuobiWsMarketApi,
    Exchange.HUOBIU.value: HuobiuWsMarketApi,
    Exchange.HUOBIS.value: HuobisWsMarketApi,
    Exchange.HUOBIF.value: HuobifWsMarketApi,
    Exchange.GATEIO.value: GateioWsMarketApi,
    Exchange.BINANCE.value: BinanceWsMarketApi,
    Exchange.BITFINEX.value: BitfinexWsMarketApi,
    Exchange.BINANCEF.value: BinancefWsMarketApi,
    Exchange.OKEX5.value: Okex5WsMarketApi
}
