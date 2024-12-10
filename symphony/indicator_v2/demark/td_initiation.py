from symphony.config import USE_MODIN
from symphony.data_classes import PriceHistory
from ..indicator_registry import IndicatorRegistry
from ..indicator_kit import IndicatorKit
from symphony.enum import Column
from typing import Optional
import pandas_ta as ta

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


def td_camouflage(price_history: PriceHistory, period: Optional[int] = 4) -> PriceHistory:
    """
    Calculates TD Camouflage.

    :param price_history: Standard price history
    :param period: Do not change
    :return: Price history with indicator applied
    """

    df = price_history.price_history

    def calc_camouflage(series: pd.Series) -> int:
        opens = df.loc[series.index, Column.OPEN]
        highs = df.loc[series.index, Column.HIGH]
        lows = df.loc[series.index, Column.LOW]
        closes = df.loc[series.index, Column.CLOSE]

        if closes[-2] > closes[-1] > opens[-1] and lows[-1] < min(lows[-3], closes[-4]):
            return 1
        if closes[-2] < closes[-1] < opens[-1] and highs[-1] > max(highs[-3], closes[-4]):
            return -1

        return 0

    df[IndicatorRegistry.TD_CAMOUFLAGE.value] = df[Column.HIGH].rolling(period + 1).apply(calc_camouflage, raw=False)
    return price_history


def td_clop(price_history: PriceHistory, period: Optional[int] = 2) -> PriceHistory:
    """
    Calculates TD Clop.

    :param price_history: Standard price history
    :param period: Do not change
    :return: Price history with indicator applied
    """

    df = price_history.price_history

    def calc_clop(series: pd.Series) -> int:
        opens = df.loc[series.index, Column.OPEN]
        highs = df.loc[series.index, Column.HIGH]
        lows = df.loc[series.index, Column.LOW]
        closes = df.loc[series.index, Column.CLOSE]

        if opens[-1] < closes[-2] < highs[-1] and opens[-1] < opens[-2] < highs[-1]:
            return 1

        if opens[-1] > closes[-2] > lows[-1] and opens[-1] > opens[-2] > lows[-1]:
            return -1

        return 0

    df[IndicatorRegistry.TD_CLOP.value] = df[Column.HIGH].rolling(period + 1).apply(calc_clop, raw=False)
    return price_history


def td_clopwin(price_history: PriceHistory, period: Optional[int] = 2) -> PriceHistory:
    """
    Calculates TD Clopwin.

    :param price_history: Standard price history
    :param period: Do not change
    :return: Price history with indicator applied
    """

    df = price_history.price_history

    def calc_clopwin(series: pd.Series) -> int:
        opens = df.loc[series.index, Column.OPEN]
        highs = df.loc[series.index, Column.HIGH]
        lows = df.loc[series.index, Column.LOW]
        closes = df.loc[series.index, Column.CLOSE]

        max_prev = max(opens[-2], closes[-2])
        min_prev = min(opens[-2], closes[-2])
        max_curr = max(opens[-1], closes[-1])
        min_curr = min(opens[-1], closes[-1])
        if max_curr < max_prev and min_curr > min_prev and closes[-1] > closes[-2]:
            return 1
        if max_curr < max_prev and min_curr > min_prev and closes[-1] < closes[-2]:
            return -1

        return 0

    df[IndicatorRegistry.TD_CLOPWIN.value] = df[Column.HIGH].rolling(period + 1).apply(calc_clopwin, raw=False)
    return price_history


def td_open(price_history: PriceHistory, period: Optional[int] = 2) -> PriceHistory:
    """
    Calculates TD Open.

    :param price_history: Standard price history
    :param period: Do not change
    :return: Price history with indicator applied
    """

    df = price_history.price_history

    def calc_open(series: pd.Series) -> int:
        opens = df.loc[series.index, Column.OPEN]
        highs = df.loc[series.index, Column.HIGH]
        lows = df.loc[series.index, Column.LOW]
        closes = df.loc[series.index, Column.CLOSE]

        if opens[-1] < lows[-2] and highs[-1] > lows[-2]:
            return 1
        if opens[-1] > highs[-2] and lows[-1] < highs[-2]:
            return -1

        return 0

    df[IndicatorRegistry.TD_OPEN.value] = df[Column.HIGH].rolling(period + 1).apply(calc_open, raw=False)
    return price_history


def td_trap(price_history: PriceHistory, period: Optional[int] = 2) -> PriceHistory:
    """
    Calculates TD Trap.

    :param price_history: Standard price history
    :param period: Do not change
    :return: Price history with indicator applied
    """

    df = price_history.price_history

    def calc_trap(series: pd.Series) -> int:
        opens = df.loc[series.index, Column.OPEN]
        highs = df.loc[series.index, Column.HIGH]
        lows = df.loc[series.index, Column.LOW]
        closes = df.loc[series.index, Column.CLOSE]

        if opens[-1] < highs[-2] and opens[-1] > lows[-2] and highs[-1] > highs[-2]:
            return 1
        if opens[-1] < highs[-2] and opens[-1] > lows[-2] and lows[-1] < lows[-2]:
            return -1

        return 0

    df[IndicatorRegistry.TD_TRAP.value] = df[Column.HIGH].rolling(period + 1).apply(calc_trap, raw=False)
    return price_history


