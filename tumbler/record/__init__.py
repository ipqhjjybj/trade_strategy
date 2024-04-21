# coding=utf-8

from tumbler.constant import Exchange

from .huobi_client import HuobiClient
from .huobif_client import HuobifClient
from .huobiu_client import HuobiuClient
from .huobis_client import HuobisClient
from .binance_client import BinanceClient
from .binancef_client import BinancefClient
from .bitmex_client import BitmexClient
from .okex_client import OkexClient
from .okexf_client import OkexfClient
from .okex5_client import Okex5Client
from .okexs_client import OkexsClient
from .nexus_client import NexusClient
from .gate_client import GateClient
from .coinex_client import CoinexClient
from .bitfinex_client import BitfinexClient

client_map = {
    Exchange.OKEX.value: OkexClient,
    Exchange.OKEXF.value: OkexfClient,
    Exchange.OKEXS.value: OkexsClient,
    Exchange.OKEX5.value: Okex5Client,
    Exchange.HUOBI.value: huobi_client,
    Exchange.HUOBIS.value: huobis_client,
    Exchange.HUOBIU.value: huobiu_client,
    Exchange.HUOBIF.value: huobif_client,
    Exchange.BINANCE.value: BinanceClient,
    Exchange.BINANCEF.value: BinancefClient,
    Exchange.NEXUS.value: NexusClient,
    Exchange.GATEIO.value: GateClient,
    Exchange.COINEX.value: CoinexClient,
    Exchange.BITFINEX.value: BitfinexClient
}
