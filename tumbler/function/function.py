# coding=utf-8
import re
from datetime import datetime, timezone, timedelta
import time

from tumbler.template.technical.util import TICKER_INTERVAL_SECONDS
from tumbler.constant import Direction


def get_no_under_lower_symbol(symbol):
    """
    etc_btc  --> etcbtc
    eth_usdt.GATEWAY --> ethusdt
    ETH_USDT.GATEWAY --> ethusdt
    """
    return (symbol.split('.'))[0].replace('_', '').lower()


target_symbols = ["cdc", "yee", "ost", "vet", "gtc", "ela", "arpa",
                  "bnb", "lrc", "get", "itc", "zla", "tnt", "trx",
                  "she", "cmt", "btt", "bts", "btc", "btm", "mds",
                  "neo", "smt", "wicc", "abt", "swftc", "cnn", "new",
                  "omg", "ast", "uuu", "let", "egcc", "mex", "qun",
                  "iris", "etc", "etf", "eth", "bch", "elf", "ong",
                  "usdt", "ont", "top", "pc", "zil", "mco", "bsv",
                  "gas", "but", "qsp", "iic", "atom", "hc", "iost",
                  "18c", "aac", "dta", "bcd", "tnb", "meet", "cvc",
                  "uc", "ae", "dash", "uip", "inc", "lamb", "aidoc",
                  "enj", "hpt", "nas", "hot", "lxt", "datx", "bsv",
                  "bifi", "bcpt", "portal", "ht", "ruff", "topc",
                  "man", "sbtc", "qtum", "eos", "gsc", "bcx", "bkbt"]

base_symbols = ["btc", "eth", "bnb", "bch", "ht", "okb", "qc", "usdk", "usdt",
                "usds", "tusd", "usdc", "busd"]

real_symbols = list(set(target_symbols + base_symbols))
global_dic = {}
for target_symbol in real_symbols:
    for base_symbol in real_symbols:
        key = '{}{}'.format(target_symbol, base_symbol)
        val = '{}_{}'.format(target_symbol, base_symbol)
        global_dic[key] = val


def get_format_system_symbol(symbol):
    return symbol.replace('_', '').lower()


def get_web_display_format_symbol(symbol):
    return symbol.replace('_', "/").upper()


def get_web_display_format_to_system_format_symbol(symbol):
    return symbol.replace('/', "_").lower()


def get_format_lower_symbol(symbol):
    """
    ethusdt --> eth_usdt
    etcbtc  --> etc_btc
    ethusdt.HUOBI --> eth_usdt
    etcbtc.HUOBI  --> etc_btc
    """
    global global_dic, base_symbols
    symbol = symbol.replace('_', '')
    symbol = ((symbol.split('.'))[0]).lower()
    n_symbol = global_dic.get(symbol, None)

    if n_symbol:
        return n_symbol
    else:
        for base_symbol in base_symbols:
            if symbol.endswith(base_symbol):
                ll = len(base_symbol)
                return '{}_{}'.format(symbol[:-ll], base_symbol)
        return '{}_{}'.format(symbol[:-3], symbol[-3:])


def get_two_currency(symbol):
    """
    ethusdt --> (eth,usdt)
    etcbtc  --> (etc,btc)
    ethusdt.HUOBI --> (eth,usdt)
    etcbtc.HUOBI  --> (etc,btc)
    """
    arr = get_format_lower_symbol(symbol).split("_")
    return (arr[0], arr[1])


def get_vt_key(symbol, exchange):
    """
    btc_usdt,exchange --> btc_usdt.exchange
    vt_key , 一些订单在 内部的时候是 local_id, 通过一定的逐渐转化，变成唯一性的key
    比如  symbol , exchange 变成 symbol.exchange , 形成唯一主键合约
    比如  order_id, exchange 变成 vt_order_id: order_id.exchange, 形成唯一订单号
    比如  exchange, account_id 变成 vt_account_id: exchange.account_id 形成唯一性账户
    """
    return "{}.{}".format(symbol, exchange)


def get_from_vt_key(vt_key):
    """
    从 vt_key 中拆出两个， 如 btc_usdt.HUOBI， 得到 btc_usdt,  HUOBI
    """
    ks = vt_key.split('.')
    return ks[0], ks[1]


def _str_url_replace(s):
    """
    替换格式
    """
    return s.replace(':', '%3A').replace('+', '%2B').replace(",", '%2C')


def urlencode(d={}):
    """
    {"12":32} --> 12=32
    {"13":32,"32":32} --> 12=32&32=32
    [(1,2)] --> 1=2
    [(1,2),(3,4)] --> 1=2&3=4
    """
    if d:
        if type(d) == list:
            return _str_url_replace('&'.join(["{}={}".format(x, y) for (x, y) in d]))

        elif type(d) == dict:
            arr = []
            for key in d.keys():
                arr.append("{}={}".format(key, d[key]))
            return _str_url_replace('&'.join(arr))

        else:
            return ""
    else:
        return ""


def split_url(url):
    """
    将url拆分为host和path
    :return: host, path
    """
    result = re.match("\w+://([^/]*)(.*)", url)  # noqa
    if result:
        return result.group(1), result.group(2)


def get_dt_use_timestamp(nt, mill=1000):
    """
    从时间戳中获得日期
    """
    return datetime.fromtimestamp(float(nt) / mill)


def get_str_dt_use_timestamp(nt, mill=1000):
    """
    从时间戳中获得日期
    """
    dt = datetime.fromtimestamp(float(nt) / mill)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def utc_2_local(utc_st):
    """
    UTC时间转本地时间（+8:00
    """
    now_stamp = time.time()
    local_time = datetime.fromtimestamp(now_stamp)
    utc_time = datetime.utcfromtimestamp(now_stamp)
    offset = local_time - utc_time
    local_st = utc_st + offset
    return local_st


def parse_timestamp(str_timestamp, pianli=0):
    """
    parse utc_tz timestamp into local time.
    """
    utc_tz = timezone.utc
    local_tz = datetime.now(timezone.utc).astimezone().tzinfo
    n_time = datetime.strptime(str_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    utc_time = n_time.replace(tzinfo=utc_tz) + timedelta(hours=pianli)
    local_time = utc_time.astimezone(local_tz)
    return local_time


def datetime_from_str_to_datetime(d):
    if isinstance(d, str):
        if len(d) > 19:
            return datetime.strptime(d, "%Y-%m-%d %H:%M:%S.%f")
        elif len(d) == 10:
            return datetime.strptime(d, "%Y-%m-%d")
        else:
            return datetime.strptime(d, "%Y-%m-%d %H:%M:%S")
    return d


def datetime_from_str_to_time(d):
    return datetime_from_str_to_datetime(d).timestamp()


def datetime_bigger(d1, d2):
    return datetime_from_str_to_datetime(d1) > datetime_from_str_to_datetime(d2)


def parse_timestamp_get_str(str_timestamp, pianli=0):
    dt = parse_timestamp(str_timestamp, pianli)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def reverse_direction(direction):
    """
    将交易方向 换掉
    """
    if direction == Direction.LONG.value:
        return Direction.SHORT.value
    else:
        return Direction.LONG.value


def get_level_token(token):
    """
    获得币种的优先级，用于确定交易对
    """
    token = token.lower()
    if token == "usdt":
        return 0
    if token == "btc":
        return 1
    if token == "eth":
        return 2
    if token == "bch":
        return 3
    return 4


def time_2_datetime(t):
    return datetime.fromtimestamp(t)


def datetime_2_time(d):
    return time.mktime(d.timetuple())


def timeframe_to_minutes(timeframe: str) -> int:
    """
    Same as timeframe_to_seconds, but returns minutes.
    """
    return timeframe_to_seconds(timeframe) // 60


def timeframe_to_seconds(timeframe: str) -> int:
    """
    Translates the timeframe interval value written in the human readable
    form ('1m', '5m', '1h', '1d', '1w', etc.) to the number
    of seconds for one timeframe interval.
    """
    return TICKER_INTERVAL_SECONDS.get(timeframe)


def get_split_num(arr, split_num=3):
    assert split_num > 0, "split should > 0"
    ret = []
    for i in range(split_num):
        ret.append([])
    n = len(arr)
    for i in range(n):
        ret[i % split_num].append(arr[i])
    return ret


def get_sum_dic(dic):
    sum_dic = {}
    ks = list(dic.keys())
    ks.sort()
    ss = 0
    for k in ks:
        ss += dic[k]
        sum_dic[k] = ss
    return sum_dic


def get_mul_sum_dic(dic):
    sum_mul_dic = {}
    ks = list(dic.keys())
    ks.sort()
    ss = 1
    for k in ks:
        ss *= (dic[k] + 1)
        sum_mul_dic[k] = ss - 1
    return sum_mul_dic


def get_sum_from_dic(dic):
    sv = 0
    for _, v in dic.items():
        sv += v
    return sv


def deep_merge_dicts(source, destination):
    """
    Values from Source override destination, destination is returned (and modified!!)
    Sample:
    >>> a = { 'first' : { 'rows' : { 'pass' : 'dog', 'number' : '1' } } }
    >>> b = { 'first' : { 'rows' : { 'fail' : 'cat', 'number' : '5' } } }
    >>> merge(b, a) == { 'first' : { 'rows' : { 'pass' : 'dog', 'fail' : 'cat', 'number' : '5' } } }
    True
    """
    for key, value in source.items():
        if isinstance(value, dict):
            # get node or create one
            node = destination.setdefault(key, {})
            deep_merge_dicts(value, node)
        else:
            destination[key] = value

    return destination

