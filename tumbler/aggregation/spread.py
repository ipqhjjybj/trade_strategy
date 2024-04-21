# coding=utf-8

from datetime import datetime, timedelta

from tumbler.constant import MAX_PRICE_NUM
from tumbler.function import datetime_bigger
import tumbler.config as config
from tumbler.event import (
    EVENT_SPREAD,
    Event
)

from tumbler.constant import MQSubscribeType
from tumbler.apps.data_third_part.base import get_diff_type_exchange_name
from tumbler.function import load_json
from tumbler.service import MQSender, log_service_manager
from tumbler.object import FutureSpotSpread

from .aggregation import Aggregation


class Spread(Aggregation):
    def __init__(self, event_engine, filename="spread_setting.json"):
        super(Spread, self).__init__(event_engine, filename)

        data = load_json(filename)
        self.monitor_spread = data.get("monitor_spread", 0.1)

        queue_exchange_name = get_diff_type_exchange_name(MQSubscribeType.FUTURE_SPOT_SPREAD_TICKER.value)
        self.sender = MQSender(host=config.SETTINGS["MQ_SERVER"], port=config.SETTINGS["MQ_PORT"],
                               user=config.SETTINGS["MQ_USER"], passwd=config.SETTINGS["MQ_PASSWD"],
                               exchange=queue_exchange_name)

    def produce_merge_ticks(self):
        # 交易所时间理论上应该 > 这个值，不然可能数据有问题
        should_after_time = datetime.now() - timedelta(seconds=10)
        for symbol in self.tot_array_ticks.keys():
            merge_tick = self.merge_ticks.get(symbol, None)
            if merge_tick is None:
                continue

            last_datetime = None
            dic = self.tot_array_ticks[symbol]

            to_sort_bids_arr = []  # (price,volume,exchange)
            to_sort_asks_arr = []  # (price,volume,exchange)
            for exchange in dic.keys():
                vt = dic[exchange]

                if datetime_bigger(vt.datetime, should_after_time):
                    if not last_datetime or datetime_bigger(vt.datetime, last_datetime):
                        last_datetime = vt.datetime

                    if vt.bid_prices[0] > 0:
                        ll = len(vt.bid_prices)
                        for i in range(ll):
                            if vt.bid_prices[i] > 0:
                                to_sort_bids_arr.append((vt.bid_prices[i], vt.bid_volumes[i], vt.exchange))

                            if vt.ask_prices[i] > 0:
                                to_sort_asks_arr.append((vt.ask_prices[i], vt.ask_volumes[i], vt.exchange))

            if len(to_sort_bids_arr) == 0:
                continue

            to_sort_bids_arr.sort(reverse=True)
            to_sort_asks_arr.sort(reverse=False)

            for i in range(min(MAX_PRICE_NUM, len(to_sort_bids_arr))):
                merge_tick.bids[i] = to_sort_bids_arr[i]

            for i in range(min(MAX_PRICE_NUM, len(to_sort_asks_arr))):
                merge_tick.asks[i] = to_sort_asks_arr[i]

            merge_tick.datetime = last_datetime

            if merge_tick.bids[0][0] and merge_tick.asks[0][0] \
                    and merge_tick.bids[0][0] > merge_tick.asks[0][0] * (1 + self.monitor_spread / 100.0):

                fs = FutureSpotSpread()
                fs.get_from_merge_tick(merge_tick)

                if self.sender is not None:
                    self.sender.send("", fs.get_mq_msg())

                    log_service_manager.write_log("[produce_merge_ticks] spread_msg:{}".format(fs.get_mq_msg()))

                e = Event(EVENT_SPREAD, fs)
                self.event_engine.put(e)


#def write_spread_setting(exchanges, )
