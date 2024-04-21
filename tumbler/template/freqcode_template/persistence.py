from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from tumbler.service import log_service_manager

from tumbler.template.constants import DATETIME_PRINT_FORMAT


class LocalTrade(object):
    """
    Trade database model.
    Used in backtesting - must be aligned to Trade model!

    """
    use_db: bool = False
    # Trades container for backtesting
    trades: List['LocalTrade'] = []
    trades_open: List['LocalTrade'] = []
    total_profit: float = 0

    id: int = 0

    exchange: str = ''
    pair: str = ''
    is_open: bool = True
    fee_open: float = 0.0
    fee_open_cost: Optional[float] = None
    fee_open_currency: str = ''
    fee_close: float = 0.0
    fee_close_cost: Optional[float] = None
    fee_close_currency: str = ''
    open_rate: float = 0.0
    open_rate_requested: Optional[float] = None
    # open_trade_value - calculated via _calc_open_trade_value
    open_trade_value: float = 0.0
    close_rate: Optional[float] = None
    close_rate_requested: Optional[float] = None
    close_profit: Optional[float] = None
    close_profit_abs: Optional[float] = None
    stake_amount: float = 0.0
    amount: float = 0.0
    amount_requested: Optional[float] = None
    open_date: datetime
    close_date: Optional[datetime] = None
    open_order_id: Optional[str] = None
    # absolute value of the stop loss
    stop_loss: float = 0.0
    # percentage value of the stop loss
    stop_loss_pct: float = 0.0
    # absolute value of the initial stop loss
    initial_stop_loss: float = 0.0
    # percentage value of the initial stop loss
    initial_stop_loss_pct: float = 0.0
    # stoploss order id which is on exchange
    stoploss_order_id: Optional[str] = None
    # last update time of the stoploss order on exchange
    stoploss_last_update: Optional[datetime] = None
    # absolute value of the highest reached price
    max_rate: float = 0.0
    # Lowest price reached
    min_rate: float = 0.0
    sell_reason: str = ''
    sell_order_status: str = ''
    strategy: str = ''
    buy_tag: Optional[str] = None
    timeframe: Optional[int] = None

    def __init__(self, **kwargs):
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def __repr__(self):
        open_since = self.open_date.strftime(DATETIME_PRINT_FORMAT) if self.is_open else 'closed'

        return (f'Trade(id={self.id}, pair={self.pair}, amount={self.amount:.8f}, '
                f'open_rate={self.open_rate:.8f}, open_since={open_since})')

    @staticmethod
    def close_bt_trade(trade):
        LocalTrade.trades_open.remove(trade)
        LocalTrade.trades.append(trade)
        LocalTrade.total_profit += trade.close_profit_abs

    @staticmethod
    def add_bt_trade(trade):
        if trade.is_open:
            LocalTrade.trades_open.append(trade)
        else:
            LocalTrade.trades.append(trade)

    @property
    def open_date_utc(self):
        return self.open_date.replace(tzinfo=timezone.utc)

    @property
    def close_date_utc(self):
        return self.close_date.replace(tzinfo=timezone.utc)

    def to_json(self) -> Dict[str, Any]:
        return {
            'trade_id': self.id,
            'pair': self.pair,
            'is_open': self.is_open,
            'exchange': self.exchange,
            'amount': round(self.amount, 8),
            'amount_requested': round(self.amount_requested, 8) if self.amount_requested else None,
            'stake_amount': round(self.stake_amount, 8),
            'strategy': self.strategy,
            'buy_tag': self.buy_tag,
            'timeframe': self.timeframe,

            'fee_open': self.fee_open,
            'fee_open_cost': self.fee_open_cost,
            'fee_open_currency': self.fee_open_currency,
            'fee_close': self.fee_close,
            'fee_close_cost': self.fee_close_cost,
            'fee_close_currency': self.fee_close_currency,

            'open_date': self.open_date.strftime(DATETIME_PRINT_FORMAT),
            'open_timestamp': int(self.open_date.replace(tzinfo=timezone.utc).timestamp() * 1000),
            'open_rate': self.open_rate,
            'open_rate_requested': self.open_rate_requested,
            'open_trade_value': round(self.open_trade_value, 8),

            'close_date': (self.close_date.strftime(DATETIME_PRINT_FORMAT)
                           if self.close_date else None),
            'close_timestamp': int(self.close_date.replace(
                tzinfo=timezone.utc).timestamp() * 1000) if self.close_date else None,
            'close_rate': self.close_rate,
            'close_rate_requested': self.close_rate_requested,
            'close_profit': self.close_profit,  # Deprecated
            'close_profit_pct': round(self.close_profit * 100, 2) if self.close_profit else None,
            'close_profit_abs': self.close_profit_abs,  # Deprecated

            'trade_duration_s': (int((self.close_date_utc - self.open_date_utc).total_seconds())
                                 if self.close_date else None),
            'trade_duration': (int((self.close_date_utc - self.open_date_utc).total_seconds() // 60)
                               if self.close_date else None),

            'profit_ratio': self.close_profit,
            'profit_pct': round(self.close_profit * 100, 2) if self.close_profit else None,
            'profit_abs': self.close_profit_abs,

            'sell_reason': self.sell_reason,
            'sell_order_status': self.sell_order_status,
            'stop_loss_abs': self.stop_loss,
            'stop_loss_ratio': self.stop_loss_pct if self.stop_loss_pct else None,
            'stop_loss_pct': (self.stop_loss_pct * 100) if self.stop_loss_pct else None,
            'stoploss_order_id': self.stoploss_order_id,
            'stoploss_last_update': (self.stoploss_last_update.strftime(DATETIME_PRINT_FORMAT)
                                     if self.stoploss_last_update else None),
            'stoploss_last_update_timestamp': int(self.stoploss_last_update.replace(
                tzinfo=timezone.utc).timestamp() * 1000) if self.stoploss_last_update else None,
            'initial_stop_loss_abs': self.initial_stop_loss,
            'initial_stop_loss_ratio': (self.initial_stop_loss_pct
                                        if self.initial_stop_loss_pct else None),
            'initial_stop_loss_pct': (self.initial_stop_loss_pct * 100
                                      if self.initial_stop_loss_pct else None),
            'min_rate': self.min_rate,
            'max_rate': self.max_rate,

            'open_order_id': self.open_order_id,
        }

    @staticmethod
    def reset_trades() -> None:
        """
        Resets all trades. Only active for backtesting mode.
        """
        LocalTrade.trades = []
        LocalTrade.trades_open = []
        LocalTrade.total_profit = 0

    def adjust_min_max_rates(self, current_price: float) -> None:
        """
        Adjust the max_rate and min_rate.
        """
        self.max_rate = max(current_price, self.max_rate or self.open_rate)
        self.min_rate = min(current_price, self.min_rate or self.open_rate)

    def _set_new_stoploss(self, new_loss: float, stoploss: float):
        """Assign new stop value"""
        self.stop_loss = new_loss
        self.stop_loss_pct = -1 * abs(stoploss)
        self.stoploss_last_update = datetime.utcnow()

    def adjust_stop_loss(self, current_price: float, stoploss: float,
                         initial: bool = False) -> None:
        """
        This adjusts the stop loss to it's most recently observed setting
        :param current_price: Current rate the asset is traded
        :param stoploss: Stoploss as factor (sample -0.05 -> -5% below current price).
        :param initial: Called to initiate stop_loss.
            Skips everything if self.stop_loss is already set.
        """
        if initial and not (self.stop_loss is None or self.stop_loss == 0):
            # Don't modify if called with initial and nothing to do
            return

        new_loss = float(current_price * (1 - abs(stoploss)))

        # no stop loss assigned yet
        if not self.stop_loss:
            log_service_manager.debug(f"{self.pair} - Assigning new stoploss...")
            self._set_new_stoploss(new_loss, stoploss)
            self.initial_stop_loss = new_loss
            self.initial_stop_loss_pct = -1 * abs(stoploss)

        # evaluate if the stop loss needs to be updated
        else:
            if new_loss > self.stop_loss:  # stop losses only walk up, never down!
                log_service_manager.debug(f"{self.pair} - Adjusting stoploss...")
                self._set_new_stoploss(new_loss, stoploss)
            else:
                log_service_manager.debug(f"{self.pair} - Keeping current stoploss...")

        log_service_manager.debug(
            f"{self.pair} - Stoploss adjusted. current_price={current_price:.8f}, "
            f"open_rate={self.open_rate:.8f}, max_rate={self.max_rate:.8f}, "
            f"initial_stop_loss={self.initial_stop_loss:.8f}, "
            f"stop_loss={self.stop_loss:.8f}. "
            f"Trailing stoploss saved us: "
            f"{float(self.stop_loss) - float(self.initial_stop_loss):.8f}.")

    def close(self, rate: float, *, show_msg: bool = True) -> None:
        """
        Sets close_rate to the given rate, calculates total profit
        and marks trade as closed
        """
        self.close_rate = rate
        self.close_profit = self.calc_profit_ratio()
        self.close_profit_abs = self.calc_profit()
        self.close_date = self.close_date or datetime.utcnow()
        self.is_open = False
        self.sell_order_status = 'closed'
        self.open_order_id = None
        if show_msg:
            log_service_manager.info(
                'Marking %s as closed as the trade is fulfilled and found no open orders for it.',
                self
            )

    def fee_updated(self, side: str) -> bool:
        """
        Verify if this side (buy / sell) has already been updated
        """
        if side == 'buy':
            return self.fee_open_currency is not None
        elif side == 'sell':
            return self.fee_close_currency is not None
        else:
            return False

    def calc_close_trade_value(self, rate: Optional[float] = None,
                               fee: Optional[float] = None) -> float:
        """
        Calculate the close_rate including fee
        :param fee: fee to use on the close rate (optional).
            If rate is not set self.fee will be used
        :param rate: rate to compare with (optional).
            If rate is not set self.close_rate will be used
        :return: Price in BTC of the open trade
        """
        if rate is None and not self.close_rate:
            return 0.0

        sell_trade = Decimal(self.amount) * Decimal(rate or self.close_rate)  # type: ignore
        fees = sell_trade * Decimal(fee or self.fee_close)
        return float(sell_trade - fees)

    def calc_profit(self, rate: Optional[float] = None,
                    fee: Optional[float] = None) -> float:
        """
        Calculate the absolute profit in stake currency between Close and Open trade
        :param fee: fee to use on the close rate (optional).
            If rate is not set self.fee will be used
        :param rate: close rate to compare with (optional).
            If rate is not set self.close_rate will be used
        :return:  profit in stake currency as float
        """
        close_trade_value = self.calc_close_trade_value(
            rate=(rate or self.close_rate),
            fee=(fee or self.fee_close)
        )
        profit = close_trade_value - self.open_trade_value
        return float(f"{profit:.8f}")

    def calc_profit_ratio(self, rate: Optional[float] = None,
                          fee: Optional[float] = None) -> float:
        """
        Calculates the profit as ratio (including fee).
        :param rate: rate to compare with (optional).
            If rate is not set self.close_rate will be used
        :param fee: fee to use on the close rate (optional).
        :return: profit ratio as float
        """
        close_trade_value = self.calc_close_trade_value(
            rate=(rate or self.close_rate),
            fee=(fee or self.fee_close)
        )
        if self.open_trade_value == 0.0:
            return 0.0
        profit_ratio = (close_trade_value / self.open_trade_value) - 1
        return float(f"{profit_ratio:.8f}")


#
# class Trade(LocalTrade):
#     """
#     Trade database model.
#     Also handles updating and querying trades
#
#     Note: Fields must be aligned with LocalTrade class
#     """
#     __tablename__ = 'trades'
#
#     use_db: bool = True
#
#     id = Column(Integer, primary_key=True)
#
#     exchange = Column(String(25), nullable=False)
#     pair = Column(String(25), nullable=False, index=True)
#     is_open = Column(Boolean, nullable=False, default=True, index=True)
#     fee_open = Column(Float, nullable=False, default=0.0)
#     fee_open_cost = Column(Float, nullable=True)
#     fee_open_currency = Column(String(25), nullable=True)
#     fee_close = Column(Float, nullable=False, default=0.0)
#     fee_close_cost = Column(Float, nullable=True)
#     fee_close_currency = Column(String(25), nullable=True)
#     open_rate = Column(Float)
#     open_rate_requested = Column(Float)
#     # open_trade_value - calculated via _calc_open_trade_value
#     open_trade_value = Column(Float)
#     close_rate = Column(Float)
#     close_rate_requested = Column(Float)
#     close_profit = Column(Float)
#     close_profit_abs = Column(Float)
#     stake_amount = Column(Float, nullable=False)
#     amount = Column(Float)
#     amount_requested = Column(Float)
#     open_date = Column(DateTime, nullable=False, default=datetime.utcnow)
#     close_date = Column(DateTime)
#     open_order_id = Column(String(255))
#     # absolute value of the stop loss
#     stop_loss = Column(Float, nullable=True, default=0.0)
#     # percentage value of the stop loss
#     stop_loss_pct = Column(Float, nullable=True)
#     # absolute value of the initial stop loss
#     initial_stop_loss = Column(Float, nullable=True, default=0.0)
#     # percentage value of the initial stop loss
#     initial_stop_loss_pct = Column(Float, nullable=True)
#     # stoploss order id which is on exchange
#     stoploss_order_id = Column(String(255), nullable=True, index=True)
#     # last update time of the stoploss order on exchange
#     stoploss_last_update = Column(DateTime, nullable=True)
#     # absolute value of the highest reached price
#     max_rate = Column(Float, nullable=True, default=0.0)
#     # Lowest price reached
#     min_rate = Column(Float, nullable=True)
#     sell_reason = Column(String(100), nullable=True)
#     sell_order_status = Column(String(100), nullable=True)
#     strategy = Column(String(100), nullable=True)
#     buy_tag = Column(String(100), nullable=True)
#     timeframe = Column(Integer, nullable=True)
#
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
