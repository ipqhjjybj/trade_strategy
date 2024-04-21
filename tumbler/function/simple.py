# coding=utf-8

from tumbler.constant import MAX_PRICE_NUM

require_min_depth = {
    "btc_": 0.1,
    "eth_": 0.1,
    "btm_": 1000,
}


def get_min_depth(symbol):
    min_volume = 0
    for key, val in require_min_depth.items():
        if symbol.startswith(key):
            min_volume = val
            break
    return min_volume


def simplify_tick(tick, bids, asks):
    bids = [(float(x[0]), float(x[1])) for x in bids]
    asks = [(float(x[0]), float(x[1])) for x in asks]

    bids = sorted(bids, key=lambda price_pair: price_pair[0], reverse=True)
    asks = sorted(asks, key=lambda price_pair: price_pair[0])

    max_num = min(MAX_PRICE_NUM, len(bids))
    for n in range(max_num):
        price, volume = bids[n]
        tick.bid_prices[n] = float(price)
        tick.bid_volumes[n] = float(volume)

    max_num = min(MAX_PRICE_NUM, len(asks))
    for n in range(max_num):
        price, volume = asks[n]
        tick.ask_prices[n] = float(price)
        tick.ask_volumes[n] = float(volume)

    tick.last_price = (tick.ask_prices[0] + tick.bid_prices[0]) / 2.0


'''
def simplify_tick(tick, bids, asks):
    bids = [(float(x[0]), float(x[1])) for x in bids]
    asks = [(float(x[0]), float(x[1])) for x in asks]

    bids = sorted(bids, key=lambda price_pair: price_pair[0], reverse=True)
    asks = sorted(asks, key=lambda price_pair: price_pair[0])

    require_volume = get_min_depth(tick.symbol)
    n_bids = len(bids)
    n = 0
    accu_volume = 0
    for i in range(n_bids):
        if n >= MAX_PRICE_NUM:
            break
        accu_volume += bids[i][1]
        if accu_volume > require_volume:
            tick.bid_prices[n] = bids[i][0]
            tick.bid_volumes[n] = accu_volume
            n = n + 1
            accu_volume = 0

    accu_volume = 0
    n = 0
    n_asks = len(asks)
    for i in range(n_asks):
        if n >= MAX_PRICE_NUM:
            break
        accu_volume += asks[i][1]
        if accu_volume > require_volume:
            tick.ask_prices[n] = asks[i][0]
            tick.ask_volumes[n] = accu_volume
            n = n + 1
            accu_volume = 0

    # max_num = min(MAX_PRICE_NUM, len(bids))
    # for n in range(max_num):
    #     price, volume = bids[n]
    #     tick.bid_prices[n] = float(price)
    #     tick.bid_volumes[n] = float(volume)

    # max_num = min(MAX_PRICE_NUM, len(asks))
    # for n in range(max_num):
    #     price, volume = asks[n]
    #     tick.ask_prices[n] = float(price)
    #     tick.ask_volumes[n] = float(volume)

    tick.last_price = (tick.ask_prices[0] + tick.bid_prices[0]) / 2.0

'''
