# coding=utf-8

from enum import Enum

from mongoengine import DateTimeField, Document, FloatField, StringField, ListField, connect

from tumbler.object import BarData, TickData
import tumbler.config as config
from tumbler.function import get_vt_key


class DbBarData(Document):
    """
    Candlestick bar data for database storage.

    Index is defined unique with datetime, interval, symbol
    """

    symbol = StringField()
    exchange = StringField()
    vt_symbol = StringField()

    open_price = FloatField()
    high_price = FloatField()
    low_price = FloatField()
    close_price = FloatField()

    date = StringField()
    time = StringField()
    datetime = DateTimeField()

    volume = FloatField()
    open_interest = FloatField()
    interval = StringField()

    meta = {
        "indexes": [
            {
                "fields": ("symbol", "exchange", "interval", "datetime"),
                "unique": True,
            }
        ]
    }

    @staticmethod
    def from_bar(bar):
        """
        Generate DbBarData object from BarData.
        """
        db_bar = DbBarData()

        db_bar.symbol = bar.symbol
        db_bar.exchange = bar.exchange.value
        db_bar.vt_symbol = bar.vt_symbol

        db_bar.open_price = bar.open_price
        db_bar.high_price = bar.high_price
        db_bar.low_price = bar.low_price
        db_bar.close_price = bar.close_price

        db_bar.date = bar.date
        db_bar.time = bar.time
        db_bar.datetime = bar.datetime

        db_bar.volume = bar.volume
        db_bar.open_interest = bar.open_interest

        return db_bar

    def to_bar(self):
        """
        Generate BarData object from DbBarData.
        """
        bar = BarData()
        bar.symbol = self.symbol
        bar.exchange = self.exchange
        bar.vt_symbol = self.vt_symbol
        if not bar.vt_symbol:
            bar.vt_symbol = get_vt_key(bar.symbol, bar.exchange)
        
        bar.open_price = self.open_price
        bar.high_price = self.high_price
        bar.low_price = self.low_price
        bar.close_price = self.close_price

        bar.date = self.date
        bar.time = self.time
        bar.datetime = self.datetime

        bar.volume = self.volume
        bar.open_interest = self.open_interest

        return bar


class DbTickData(Document):
    """
    Tick data for database storage.

    Index is defined unique with (datetime, symbol)
    """

    symbol = StringField()
    exchange = StringField()
    vt_symbol = StringField()

    name = StringField()
    gateway_name = StringField()

    last_price = FloatField()
    last_volume = FloatField()
    volume = FloatField()

    open_interest = FloatField()

    time = StringField()
    date = StringField()
    datetime = DateTimeField()

    upper_limit = FloatField()
    lower_limit = FloatField()

    bid_prices = ListField(FloatField())
    ask_prices = ListField(FloatField())

    bid_volumes = ListField(FloatField())
    ask_volumes = ListField(FloatField())

    meta = {
        "indexes": [
            {
                "fields": ("symbol", "exchange", "datetime"),
                "unique": True,
            }
        ],
    }

    @staticmethod
    def from_tick(tick):
        """
        Generate DbTickData object from TickData.
        """
        db_tick = DbTickData()

        db_tick.symbol = tick.symbol
        db_tick.exchange = tick.exchange.value
        db_tick.vt_symbol = tick.vt_symbol

        db_tick.name = tick.name
        db_tick.gateway_name = tick.gateway_name

        db_tick.last_price = tick.last_price
        db_tick.last_volume = tick.last_volume
        db_tick.volume = tick.volume

        db_tick.open_interest = tick.open_interest

        db_tick.time = tick.time
        db_tick.date = tick.date

        db_tick.upper_limit = tick.upperLimit
        db_tick.lower_limit = tick.lowerLimit

        db_tick.bid_prices = tick.bid_prices
        db_tick.ask_prices = tick.ask_prices

        db_tick.bid_volumes = tick.bid_volumes
        db_tick.ask_volumes = tick.ask_volumes

        return db_tick

    def to_tick(self):
        """
        Generate TickData object from DbTickData.
        """
        tick = TickData()
        tick.symbol = self.symbol
        tick.exchange = self.exchange
        tick.vt_symbol = self.vt_symbol
        if not tick.vt_symbol:
            tick.vt_symbol = get_vt_key(tick.symbol, tick.exchange)

        tick.name = self.name
        tick.gateway_name = self.gateway_name

        tick.last_price = self.last_price
        tick.last_volume = self.last_volume
        tick.volume = self.volume

        tick.open_interest = self.open_interest

        tick.time = self.time
        tick.date = self.date
        tick.datetime = self.datetime

        tick.upper_limit = self.upper_limit
        tick.lower_limit = self.lower_limit

        tick.bid_prices = self.bid_prices
        tick.ask_prices = self.ask_prices

        return tick


class MongoService(object):

    def load_bar_data(self, symbol, exchange, interval, start, end):
        s = DbBarData.objects(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            datetime__gte=start,
            datetime__lte=end,
        )
        data = [db_bar.to_bar() for db_bar in s]
        return data

    def load_tick_data(self, symbol, exchange, start, end):
        s = DbTickData.objects(
            symbol=symbol,
            exchange=exchange,
            datetime__gte=start,
            datetime__lte=end,
        )
        data = [db_tick.to_tick() for db_tick in s]
        return data

    @staticmethod
    def to_update_param(d):
        return {
            "set__" + k: v.value if isinstance(v, Enum) else v
            for k, v in d.__dict__.items()
        }

    def save_bar_data(self, datas):
        for d in datas:
            updates = self.to_update_param(d)
            updates.pop("set__gateway_name")
            updates.pop("set__vt_symbol")
            (
                DbBarData.objects(
                    symbol=d.symbol, interval=d.interval, datetime=d.datetime
                ).update_one(upsert=True, **updates)
            )

    def save_tick_data(self, datas):
        for d in datas:
            updates = self.to_update_param(d)
            updates.pop("set__gateway_name")
            updates.pop("set__vt_symbol")
            (
                DbTickData.objects(
                    symbol=d.symbol, exchange=d.exchange, datetime=d.datetime
                ).update_one(upsert=True, **updates)
            )

    def get_newest_bar_data(self, symbol, exchange, interval):
        s = (
            DbBarData.objects(symbol=symbol, exchange=exchange)
                .order_by("-datetime")
                .first()
        )
        if s:
            return s.to_bar()
        return None

    def get_newest_tick_data(self, symbol, exchange):
        s = (
            DbTickData.objects(symbol=symbol, exchange=exchange)
                .order_by("-datetime")
                .first()
        )
        if s:
            return s.to_tick()
        return None

    def clean(self, symbol):
        DbTickData.objects(symbol=symbol).delete()
        DbBarData.objects(symbol=symbol).delete()


def init():
    database = config.SETTINGS["mongodb_database"]
    host = config.SETTINGS["mongodb_host"]
    port = config.SETTINGS["mongodb_port"]
    username = config.SETTINGS["mongodb_user"]
    password = config.SETTINGS["mongodb_password"]
    authentication_source = config.SETTINGS["mongodb_authentication_source"]

    if not username:  # if username == '' or None, skip username
        username = None
        password = None
        authentication_source = None

    connect(
        db=database,
        host=host,
        port=port,
        username=username,
        password=password,
        authentication_source=authentication_source,
    )

    return MongoService()


mongo_service_manager = init()
