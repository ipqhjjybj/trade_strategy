# pragma pylint: disable=missing-docstring, W0212, too-many-arguments

"""
This module contains the backtesting logic
"""
import logging
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from pandas import DataFrame

from tumbler.template.freqcode_template.exchange import trade_list_to_dataframe
from tumbler.template.freqcode_template.enums import SellType
from tumbler.template.freqcode_template.persistence import LocalTrade
from tumbler.template.freqcode_template.interface import IStrategy, SellCheckTuple
from tumbler.template.freqcode_template.interface import strategy_safe_wrapper
from tumbler.function import timeframe_to_minutes
#

# Indexes for backtest tuples
DATE_IDX = 0
BUY_IDX = 1
OPEN_IDX = 2
CLOSE_IDX = 3
SELL_IDX = 4
LOW_IDX = 5
HIGH_IDX = 6
BUY_TAG_IDX = 7


class Backtesting:
    """
    Backtesting class, this class contains all the logic to run a backtest

    To run a backtest:
    backtesting = Backtesting(config)
    backtesting.start()
    """

    def __init__(self, strategy) -> None:
        self.strategy = strategy
        self.timeframe_min = timeframe_to_minutes(strategy.timeframe)
        self.fee = 0.001
        self.rejected_trades = 0

    def _get_close_rate(self, sell_row: Tuple, trade: LocalTrade, sell: SellCheckTuple,
                        trade_dur: int) -> float:
        """
        Get close rate for backtesting result
        """
        # Special handling if high or low hit STOP_LOSS or ROI
        if sell.sell_type in (SellType.STOP_LOSS, SellType.TRAILING_STOP_LOSS):
            if trade.stop_loss > sell_row[HIGH_IDX]:
                # our stoploss was already higher than candle high,
                # possibly due to a cancelled trade exit.
                # sell at open price.
                return sell_row[OPEN_IDX]

            # Special case: trailing triggers within same candle as trade opened. Assume most
            # pessimistic price movement, which is moving just enough to arm stoploss and
            # immediately going down to stop price.
            if sell.sell_type == SellType.TRAILING_STOP_LOSS and trade_dur == 0:
                if (
                    not self.strategy.use_custom_stoploss and self.strategy.trailing_stop
                    and self.strategy.trailing_only_offset_is_reached
                    and self.strategy.trailing_stop_positive_offset is not None
                    and self.strategy.trailing_stop_positive
                ):
                    # Worst case: price reaches stop_positive_offset and dives down.
                    stop_rate = (sell_row[OPEN_IDX] *
                                 (1 + abs(self.strategy.trailing_stop_positive_offset) -
                                  abs(self.strategy.trailing_stop_positive)))
                else:
                    # Worst case: price ticks tiny bit above open and dives down.
                    stop_rate = sell_row[OPEN_IDX] * (1 - abs(trade.stop_loss_pct))
                    assert stop_rate < sell_row[HIGH_IDX]
                return stop_rate

            # Set close_rate to stoploss
            return trade.stop_loss
        elif sell.sell_type == (SellType.ROI):
            roi_entry, roi = self.strategy.min_roi_reached_entry(trade_dur)
            if roi is not None and roi_entry is not None:
                if roi == -1 and roi_entry % self.timeframe_min == 0:
                    # When forceselling with ROI=-1, the roi time will always be equal to trade_dur.
                    # If that entry is a multiple of the timeframe (so on candle open)
                    # - we'll use open instead of close
                    return sell_row[OPEN_IDX]

                # - (Expected abs profit + open_rate + open_fee) / (fee_close -1)
                close_rate = - (trade.open_rate * roi + trade.open_rate *
                                (1 + trade.fee_open)) / (trade.fee_close - 1)

                if (trade_dur > 0 and trade_dur == roi_entry
                        and roi_entry % self.timeframe_min == 0
                        and sell_row[OPEN_IDX] > close_rate):
                    # new ROI entry came into effect.
                    # use Open rate if open_rate > calculated sell rate
                    return sell_row[OPEN_IDX]

                # Use the maximum between close_rate and low as we
                # cannot sell outside of a candle.
                # Applies when a new ROI setting comes in place and the whole candle is above that.
                return min(max(close_rate, sell_row[LOW_IDX]), sell_row[HIGH_IDX])

            else:
                # This should not be reached...
                return sell_row[OPEN_IDX]
        else:
            return sell_row[OPEN_IDX]

    def _get_sell_trade_entry(self, trade: LocalTrade, sell_row: Tuple) -> Optional[LocalTrade]:
        sell = self.strategy.should_sell(trade, sell_row[OPEN_IDX],  # type: ignore
                                         sell_row[DATE_IDX].to_pydatetime(), sell_row[BUY_IDX],
                                         sell_row[SELL_IDX],
                                         low=sell_row[LOW_IDX], high=sell_row[HIGH_IDX])

        if sell.sell_flag:
            trade.close_date = sell_row[DATE_IDX].to_pydatetime()
            trade.sell_reason = sell.sell_reason
            trade_dur = int((trade.close_date_utc - trade.open_date_utc).total_seconds() // 60)
            closerate = self._get_close_rate(sell_row, trade, sell, trade_dur)

            # Confirm trade exit:
            time_in_force = self.strategy.order_time_in_force['sell']
            if not strategy_safe_wrapper(self.strategy.confirm_trade_exit, default_retval=True)(
                    pair=trade.pair, trade=trade, order_type='limit', amount=trade.amount,
                    rate=closerate,
                    time_in_force=time_in_force,
                    sell_reason=sell.sell_reason,
                    current_time=sell_row[DATE_IDX].to_pydatetime()):
                return None

            trade.close(closerate, show_msg=False)
            return trade

        return None

    def _enter_trade(self, pair: str, row: List) -> Optional[LocalTrade]:
        stake_amount = 1

        order_type = self.strategy.order_types['buy']
        time_in_force = self.strategy.order_time_in_force['sell']
        # Confirm trade entry:
        if not strategy_safe_wrapper(self.strategy.confirm_trade_entry, default_retval=True)(
                pair=pair, order_type=order_type, amount=stake_amount, rate=row[OPEN_IDX],
                time_in_force=time_in_force, current_time=row[DATE_IDX].to_pydatetime()):
            return None

        # Enter trade
        has_buy_tag = len(row) >= BUY_TAG_IDX + 1
        trade = LocalTrade(
            pair=pair,
            open_rate=row[OPEN_IDX],
            open_date=row[DATE_IDX].to_pydatetime(),
            stake_amount=stake_amount,
            amount=round(stake_amount / row[OPEN_IDX], 8),
            fee_open=self.fee,
            fee_close=self.fee,
            is_open=True,
            buy_tag=row[BUY_TAG_IDX] if has_buy_tag else None,
            exchange='backtesting',
        )
        return trade

    def handle_left_open(self, open_trades: Dict[str, List[LocalTrade]],
                         data: Dict[str, List[Tuple]]) -> List[LocalTrade]:
        """
        Handling of left open trades at the end of backtesting
        """
        trades = []
        for pair in open_trades.keys():
            if len(open_trades[pair]) > 0:
                for trade in open_trades[pair]:
                    sell_row = data[pair][-1]

                    trade.close_date = sell_row[DATE_IDX].to_pydatetime()
                    trade.sell_reason = SellType.FORCE_SELL.value
                    trade.close(sell_row[OPEN_IDX], show_msg=False)
                    LocalTrade.close_bt_trade(trade)
                    # Deepcopy object to have wallets update correctly
                    trade1 = deepcopy(trade)
                    trade1.is_open = True
                    trades.append(trade1)
        return trades

    def trade_slot_available(self, max_open_trades: int, open_trade_count: int) -> bool:
        # Always allow trades when max_open_trades is enabled.
        if max_open_trades <= 0 or open_trade_count < max_open_trades:
            return True
        # Rejected trade
        self.rejected_trades += 1
        return False

    def _get_ohlcv_as_lists(self, processed: Dict[str, DataFrame]) -> Dict[str, Tuple]:
        """
        Helper function to convert a processed dataframes into lists for performance reasons.

        Used by backtest() - so keep this optimized for performance.
        """
        # Every change to this headers list must evaluate further usages of the resulting tuple
        # and eventually change the constants for indexes at the top
        headers = ['datetime', 'buy', 'open', 'close', 'sell', 'low', 'high']
        data: Dict = {}

        # Create dict with data
        for pair, pair_data in processed.items():
            has_buy_tag = 'buy_tag' in pair_data
            headers = headers + ['buy_tag'] if has_buy_tag else headers
            if not pair_data.empty:
                pair_data.loc[:, 'buy'] = 0  # cleanup if buy_signal is exist
                pair_data.loc[:, 'sell'] = 0  # cleanup if sell_signal is exist
                if has_buy_tag:
                    pair_data.loc[:, 'buy_tag'] = None  # cleanup if buy_tag is exist

            df_analyzed = self.strategy.advise_sell(
                self.strategy.advise_buy(
                    self.strategy.advise_indicators(
                        pair_data, {'pair': pair}
                    ),
                    {'pair': pair}
                ),
                {'pair': pair}
            ).copy()

            # To avoid using data from future, we use buy/sell signals shifted
            # from the previous candle
            df_analyzed.loc[:, 'buy'] = df_analyzed.loc[:, 'buy'].shift(1)
            df_analyzed.loc[:, 'sell'] = df_analyzed.loc[:, 'sell'].shift(1)
            if has_buy_tag:
                df_analyzed.loc[:, 'buy_tag'] = df_analyzed.loc[:, 'buy_tag'].shift(1)

            df_analyzed.drop(df_analyzed.head(1).index, inplace=True)

            df_analyzed.to_csv("test.csv")

            # Convert from Pandas to list for performance reasons
            # (Looping Pandas is slow.)
            data[pair] = df_analyzed[headers].values.tolist()
        return data

    def backtest(self, processed: Dict,
                 start_date: datetime, end_date: datetime,
                 max_open_trades: int = 1000000) -> Dict[str, Any]:
        """
        Implement backtesting functionality

        NOTE: This method is used by Hyperopt at each iteration. Please keep it optimized.
        Of course try to not have ugly code. By some accessor are sometime slower than functions.
        Avoid extensive logging in this method and functions it calls.

        :param processed: a processed dictionary with format {pair, data}
        :param start_date: backtesting timerange start datetime
        :param end_date: backtesting timerange end datetime
        :param max_open_trades: maximum number of concurrent trades, <= 0 means unlimited
        :return: DataFrame with trades (results of backtesting)
        """
        trades: List[LocalTrade] = []
        data: Dict = self._get_ohlcv_as_lists(processed)
        # Indexes per pair, so some pairs are allowed to have a missing start.
        indexes: Dict = defaultdict(int)
        tmp = start_date + timedelta(minutes=self.timeframe_min)

        open_trades: Dict[str, List[LocalTrade]] = defaultdict(list)
        open_trade_count = 0

        # Loop timerange and get candle for each pair at that point in time
        while tmp <= end_date:
            open_trade_count_start = open_trade_count
            for i, pair in enumerate(data):
                row_index = indexes[pair]
                try:
                    row = data[pair][row_index]
                except IndexError:
                    # missing Data for one pair at the end.
                    # Warnings for this are shown during data loading
                    continue

                #print(row_index, row, tmp)
                # Waits until the time-counter reaches the start of the data for this pair.
                if row[DATE_IDX] > tmp:
                    continue

                row_index += 1
                indexes[pair] = row_index

                # without positionstacking, we can only have one open trade per pair.
                # max_open_trades must be respected
                # don't open on the last row
                if (
                    (len(open_trades[pair]) == 0)
                    and self.trade_slot_available(max_open_trades, open_trade_count_start)
                    and tmp != end_date
                    and row[BUY_IDX] == 1
                    and row[SELL_IDX] != 1
                ):
                    trade = self._enter_trade(pair, row)
                    if trade:
                        print("_enter_trade trade:", trade.__dict__)
                        # TODO: hacky workaround to avoid opening > max_open_trades
                        # This emulates previous behaviour - not sure if this is correct
                        # Prevents buying if the trade-slot was freed in this candle
                        open_trade_count_start += 1
                        open_trade_count += 1
                        # logger.debug(f"{pair} - Emulate creation of new trade: {trade}.")
                        open_trades[pair].append(trade)
                        LocalTrade.add_bt_trade(trade)

                for trade in list(open_trades[pair]):
                    # also check the buying candle for sell conditions.
                    trade_entry = self._get_sell_trade_entry(trade, row)
                    # Sell occurred
                    if trade_entry:
                        print("_get_sell_trade_entry trade:", trade_entry.__dict__)
                        # logger.debug(f"{pair} - Backtesting sell {trade}")
                        open_trade_count -= 1
                        open_trades[pair].remove(trade)

                        LocalTrade.close_bt_trade(trade)
                        trades.append(trade_entry)

            tmp += timedelta(minutes=self.timeframe_min)

        trades += self.handle_left_open(open_trades, data=data)

        results = trade_list_to_dataframe(trades)
        return {
            'results': results,
            'config': self.strategy.config,
            'rejected_signals': self.rejected_trades,
        }

    def get_pos_signal(self):
        pass
