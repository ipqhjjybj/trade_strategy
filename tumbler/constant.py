# coding=utf-8

from enum import Enum

MIN_FLOAT_VAL = 1e-10

MAX_PRICE_NUM = 50
EMPTY_STRING = ""
EMPTY_INT = 0
EMPTY_FLOAT = 0.0
EMPTY_UNICODE = ""


class MarginMode(Enum):
    """
    永续合约的种类，支持全部
    """
    ALL = "all"
    ISOLATED = "isolated"


class TradeOrderSendType(Enum):
    """
    下到指定仓位时， 是优先市价单还是 限价单
    """
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    POST_ONLY = "POST_ONLY"


class CheckTradeAccountType(Enum):
    """
    下单前，检查仓位的类型
    """
    NOT_CHECK_ACCOUNT = "NOT"
    CHECK_ACCOUNT = "CHECK"


class EvalType(Enum):
    """
    eval 执行的类型
    """
    SIMPLE_FUNC = 1
    PCT_STOP_FUNC = 2
    ATR_STOP_FUNC = 3
    MANY_CONDITIONS_FUNC = 4


class SignalType(Enum):
    """
    信号的类型
    """
    BPK = 1  # 多平开， 不管仓位是多少，都变成 1
    SPK = -1  # 空平开， 不管仓位是多少，都变成 -1
    BP = -2  # 多平，如果仓位是1，则变成0
    SP = 2  # 空平，如果仓位是-1，则变成0


class DiffTypeFuture(Enum):
    """
    期货的类型，当周，次周，季度，下季度
    """
    THIS_WEEK = "this_week"
    NEXT_WEEK = "next_week"
    THIS_QUARTER = "quarter"
    NEXT_QUARTER = "bi_quarter"


class MQCommonInfo(Enum):
    """
    Common information
    """
    COVER_ALL_ACCOUNT = "cover-all"


class MQDataType(Enum):
    """
    Type of mq data type received
    """
    TICKER = "T"
    BBO_TICKER = "B"
    MERGE_TICKER = "M"
    ORDER = "O"
    POSITION = "P"
    ACCOUNT = "A"
    DICT_ACCOUNT = "DA"
    SEND_ORDER = "S"
    TRANSFER = "G"
    TRADE_DATA = "TO"
    COVER_ORDER_REQUEST = "CR"
    REJECT_COVER_ORDER_REQUEST = "RCR"
    CONTRACT_DATA = "C"
    UNKNOWN_DATA = "U"
    FUTURE_SPOT_SPREAD = "SF"


class TradeType(Enum):
    """
    Type of Trade Data
    """
    EMPTY = ""
    PUT_ORDER = "P"
    COVER_ORDER = "C"


class MakerControlType(Enum):
    """
    Type of maker control type
    """
    DIRECT = "DIRECT"
    BUY_SELL = "BUY_SELL"


class MQSubscribeType(Enum):
    """
    Type of rabbitmq subscribe type
    """
    NONE = ""

    # order
    SEND_ORDER = "SEND_ORDER"

    # trade
    TRADE_DATA = "TRADE_DATA"

    # request
    COVER_ORDER_REQUEST = "COVER_ORDER_REQ"
    REJECT_COVER_ORDER_REQUEST = "REJECT_COVER_ORDER_REQ"

    # account
    ACCOUNT = "ACCOUNT"
    DICT_ACCOUNT = "DICT_ACCOUNT"

    # ticker
    MERGE_TICKER = "MERGE_TICKER"
    TIKER = "TICKER"
    BBO_TICKER = "BBO_TICKER"

    # spread
    FUTURE_SPOT_SPREAD_TICKER = "SPREAD_TICKER"


class RunMode(Enum):
    """
    Statement of RunMode
    """
    QUERY = "QUERY"
    COVER = "COVER"
    PUT_ORDER = "PUT_ORDER"
    CONTROL = "CONTROL"
    NORMAL = "NORMAL"


class NrpeState(Enum):
    """
    Statement of Nrpe
    """
    STATE_OK = 0
    STATE_WARNING = 1
    STATE_CRITICAL = 2
    STATE_UNKNOWN = 3


class Direction(Enum):
    """
    Direction of order/trade/position.
    """
    LONG = "LONG"  # 多
    SHORT = "SHORT"  # 空
    NET = "NET"  # 净头寸，用于现货
    BOTH = "BOTH"  # 未指定方向
    FORBID = "FORBID"  # 禁止交易该品种


class Offset(Enum):
    """
    Offset of order/trade.
    """
    NONE = ""
    OPEN = "OPEN"  # 开仓
    CLOSE = "CLOSE"  # 平仓


class StopOrderStatus(Enum):
    WAITING = "WAITING"  # 等待
    CANCELLED = "CANCELLED"  # 已撤销
    TRIGGERED = "TRIGGERED"  # 已触发


class Status(Enum):
    """
    Order status.
    """
    SUBMITTING = "SUBMITTING"  # 已提交
    NOTTRADED = "NOTTRADED"  # 未成交
    PARTTRADED = "PARTTRADED"  # 部分成交
    ALLTRADED = "ALLTRADED"  # 全部成交
    CANCELLED = "CANCELLED"  # 已撤销
    REJECTED = "REJECTED"  # 拒绝


class Product(Enum):
    """
    Product class.
    """
    SPOT = "SPOT"  # 现货
    FUTURES = "FUTURES"  # 期货
    OPTIONS = "OPTIONS"  # 期权


class OrderType(Enum):
    """
    Order type.
    """
    LIMIT = "LIMIT"  # 现价单
    MARKET = "MARKET"  # 市价单
    POST_ONLY = "POST_ONLY"  # 只做maker单
    STOP = "STOP"  # STOP单
    FAK = "FAK"  # FAK单
    FOK = "FOK"  # FOK单
    IOC = "IOC"  # IOC单，立即成交并取消剩余


class Exchange(Enum):
    """
    Exchange.
    """
    # Inside
    AGGREGATION = "AGGR"  # AGGREGATION value
    COVER = "COVER"  # all cover account's sum amount : ["btc.COVER", "usdt.COVER"]
    INFO = "INFO"  # For produce information
    ALL = "ALL"  # ALL account sum

    # MOV 
    MOV = "MOV"  # mov 现货交易
    FLASH = "FLASH"  # mov 闪兑部分
    SUPER = "SUPER"  # mov 超导兑换

    NEXUS = "NEXUS"  # NEXUS 交易所

    # CryptoCurrency
    BITMEX = "BITMEX"
    OKEX5 = "OKEX5"
    OKEX = "OKEX"
    OKEXF = "OKEXF"  # OKEX 交割合约
    OKEXS = "OKEXS"  # OKEX 永续合约
    HUOBI = "HUOBI"
    HUOBIS = "HUOBIS"  # 火币币本位永续合约
    HUOBIU = "HUOBIU"  # 火币USDT永续的数据
    HUOBIF = "HUOBIF"  # 火币交割永续合约
    BITFINEX = "BITFINEX"
    BINANCE = "BINANCE"
    BINANCEF = "BINANCEF"
    COINBASE = "COINBASE"
    GATEIO = "GATEIO"
    COINEX = "COINEX"
    COINEXS = "COINEXS"

    BITTREX = "BITTREX"

    UNISWAP = "UNISWAP"
    PANCAKE = "PANCAKE"
    BMC = "BMC"

    # Special Function
    LOCAL = "LOCAL"  # For local generated data


class Interval(Enum):
    """
    Interval of bar data.
    """
    TICK = "1t"

    MINUTE = "1m"
    MINUTE3 = "3m"
    MINUTE5 = "5m"
    MINUTE15 = "15m"
    MINUTE30 = "30m"
    HOUR = "1h"
    HOUR2 = "2h"
    HOUR4 = "4h"
    HOUR6 = "6h"
    HOUR12 = "12h"
    DAY = "1d"
    WEEK = "1w"
    MONTH = "1M"
    MONTH3 = "3M"
    MONTH6 = "6M"
    YEAR = '1y'
