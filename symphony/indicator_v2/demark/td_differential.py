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


def td_differential(price_history: PriceHistory, period: Optional[int] = 2) -> PriceHistory:
    """
    Calculates TD Differential. 1 for up, -1 for down

    :param price_history: Standard price history
    :param period: Do not change
    :return: Price history with indicator applied
    """

    df = price_history.price_history

    def calc_differential(series: pd.Series) -> int:
        highs = df.loc[series.index, Column.HIGH]
        lows = df.loc[series.index, Column.LOW]
        closes = df.loc[series.index, Column.CLOSE]

        if closes[-3] > closes[-2] > closes[-1]:
            buying_pressure_curr = __buying_pressure(closes, lows, -1)
            buying_pressure_prev = __buying_pressure(closes, lows, -2)
            if buying_pressure_curr > buying_pressure_prev:
                selling_pressure_curr = __selling_pressure(closes, highs, -1)
                selling_pressure_prev = __selling_pressure(closes, highs, -2)
                if selling_pressure_curr < selling_pressure_prev:
                    return 1

        if closes[-3] < closes[-2] < closes[-1]:
            selling_pressure_curr = __selling_pressure(closes, highs, -1)
            selling_pressure_prev = __selling_pressure(closes, highs, -2)
            if selling_pressure_curr > selling_pressure_prev:
                buying_pressure_curr = __buying_pressure(closes, lows, -1)
                buying_pressure_prev = __buying_pressure(closes, lows, -2)
                if buying_pressure_curr < buying_pressure_prev:
                    return -1
        return 0

    df[IndicatorRegistry.TD_DIFFERENTIAL.value] = df[Column.HIGH].rolling(period + 1).apply(calc_differential, raw=False)
    return price_history


def td_reverse_differential(price_history: PriceHistory, period: Optional[int] = 2) -> PriceHistory:
    """
    Calculates TD Reverse Differential. 1 for up, -1 for down

    :param price_history: Standard price history
    :param period: Do not change
    :return: Price history with indicator applied
    """

    df = price_history.price_history

    def calc_reverse_differential(series: pd.Series) -> int:
        highs = df.loc[series.index, Column.HIGH]
        lows = df.loc[series.index, Column.LOW]
        closes = df.loc[series.index, Column.CLOSE]
        if closes[-3] > closes[-2] > closes[-1]:
            buying_pressure_curr = __buying_pressure(closes, lows, -1)
            buying_pressure_prev = __buying_pressure(closes, lows, -2)
            if buying_pressure_curr < buying_pressure_prev:
                selling_pressure_curr = __selling_pressure(closes, highs, -1)
                selling_pressure_prev = __selling_pressure(closes, highs, -2)
                if selling_pressure_curr > selling_pressure_prev:
                    return -1

        if closes[-3] < closes[-2] < closes[-1]:
            selling_pressure_curr = __selling_pressure(closes, highs, -1)
            selling_pressure_prev = __selling_pressure(closes, highs, -2)
            if selling_pressure_curr < selling_pressure_prev:
                buying_pressure_curr = __buying_pressure(closes, lows, -1)
                buying_pressure_prev = __buying_pressure(closes, lows, -2)
                if buying_pressure_curr > buying_pressure_prev:
                    return 1
        return 0

    df[IndicatorRegistry.TD_REVERSE_DIFFERENTIAL.value] = df[Column.HIGH].rolling(period + 1).apply(calc_reverse_differential, raw=False)
    return price_history


def td_anti_differential(price_history: PriceHistory, period: Optional[int] = 4) -> PriceHistory:
    """
    Calculates TD Anti-Differential. 1 for up, -1 for down

    :param price_history: Standard price history
    :param period: Do not change
    :return: Price history with indicator applied
    """

    df = price_history.price_history

    def calc_anti_differential(series: pd.Series) -> int:
        closes = df.loc[series.index, Column.CLOSE]

        if closes[-5] < closes[-4] < closes[-3]:
            if closes[-2] > closes[-3] and closes[-1] < closes[-2]:
                return 1
        if closes[-5] > closes[-4] > closes[-3]:
            if closes[-2] < closes[-3] and closes[-1] > closes[-2]:
                return -1
        return 0

    df[IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value] = df[Column.HIGH].rolling(period + 1).apply(calc_anti_differential, raw=False)
    return price_history


def __buying_pressure(close_series: pd.Series, low_series: pd.Series, index: int) -> float:
    return close_series[index] - min(low_series[index], close_series[index - 1])


def __selling_pressure(close_series: pd.Series, high_series: pd.Series, index: int) -> float:
    return max(high_series[index], close_series[index - 1]) - close_series[index]
