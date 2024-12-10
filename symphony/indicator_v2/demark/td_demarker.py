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


def td_demarker_I(price_history: PriceHistory, period: Optional[int] = 13) -> PriceHistory:
    """
    Implements TD Demarker I

    :param price_history: Standard price history
    :param period: Default 13
    :return: Price history with indicator
    """
    df = price_history.price_history

    def calc_demarker(series: pd.Series) -> float:
        highs = df.loc[series.index, Column.HIGH]
        lows = df.loc[series.index, Column.LOW]

        high_sums = []
        low_sums = []
        for i in range(-1, -1 - period, -1):
            if highs[i] >= highs[i - 1]:
                high_sums.append(abs(highs[i] - highs[i - 1]))
            else:
                high_sums.append(0)

            if lows[i] <= lows[i - 1]:
                low_sums.append(abs(lows[i] - lows[i - 1]))
            else:
                low_sums.append(0)

        val = sum(high_sums) / (sum(high_sums) + sum(low_sums))
        return val
    df[IndicatorRegistry.TD_DEMARKER_I.value] = df[Column.HIGH].rolling(period + 1).apply(calc_demarker, raw=False)
    return price_history


def td_demarker_II(price_history: PriceHistory, period: Optional[int] = 8) -> PriceHistory:
    """
    Implements TD Demarker II

    :param price_history: Standard price history
    :param period: Default 8
    :return: Price history with indicator
    """
    df = price_history.price_history

    def calc_demarker_ii(series: pd.Series) -> float:
        highs = df.loc[series.index, Column.HIGH]
        lows = df.loc[series.index, Column.LOW]
        closes = df.loc[series.index, Column.CLOSE]

        numerator_sums = []

        denominator_sums = []
        for i in range(-1, -1 - period, -1):

            hc_diff = highs[i] - closes[i - 1]
            hc_diff += closes[i] - IndicatorKit.get_true_low(price_history, i)
            if highs[i] - closes[i - 1] < 0:
                numerator_sums.append(0)
            else:
                numerator_sums.append(hc_diff)

            denominator_sum = 0
            denominator_sum += abs(lows[i] - closes[i - 1])
            denominator_sum += abs(IndicatorKit.get_true_high(price_history, i) - closes[i])

            if closes[i - 1] - IndicatorKit.get_true_low(price_history, i) < 0.0:
                denominator_sums.append(0)
            else:
                denominator_sums.append(denominator_sum)

        numerator = sum(numerator_sums)
        denominator = sum(denominator_sums) + numerator
        return numerator / denominator

    df[IndicatorRegistry.TD_DEMARKER_II.value] = df[Column.HIGH].rolling(period + 1).apply(calc_demarker_ii, raw=False)
    return price_history
