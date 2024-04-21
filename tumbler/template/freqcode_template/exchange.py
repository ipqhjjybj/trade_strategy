import pandas as pd

from typing import List

from tumbler.template.persistence import LocalTrade
from tumbler.template.constants import BT_DATA_COLUMNS


def trade_list_to_dataframe(trades: List[LocalTrade]) -> pd.DataFrame:
    """
    Convert list of Trade objects to pandas Dataframe
    :param trades: List of trade objects
    :return: Dataframe with BT_DATA_COLUMNS
    """
    df = pd.DataFrame.from_records([t.to_json() for t in trades], columns=BT_DATA_COLUMNS)
    if len(df) > 0:
        df.loc[:, 'close_date'] = pd.to_datetime(df['close_date'], utc=True)
        df.loc[:, 'open_date'] = pd.to_datetime(df['open_date'], utc=True)
        df.loc[:, 'close_rate'] = df['close_rate'].astype('float64')
    return df
