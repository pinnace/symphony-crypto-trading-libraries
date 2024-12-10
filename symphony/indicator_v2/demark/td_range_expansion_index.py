from symphony.config import USE_MODIN
from symphony.data_classes import PriceHistory
from symphony.enum.timeframe import timeframe_to_numpy_string
from symphony.indicator_v2.indicator_registry import IndicatorRegistry
from symphony.enum import Column
import numpy as np
from typing import Optional

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


def td_range_expansion_index(price_history: PriceHistory, period: Optional[int] = 5) -> PriceHistory:
    """
    Calculates the Range Expansion Index

    :param price_history: Standard price history
    :param period: Optionally define period
    :return: PriceHistory
    """
    df = price_history.price_history
    df[IndicatorRegistry.TD_RANGE_EXPANSION_INDEX.value] = np.nan

    cond1 = lambda i, highs, lows: highs[i] >= lows[i - 5] or highs[i] >= lows[i - 6]
    cond2 = lambda i, highs, closes: highs[i - 2] >= closes[i - 7] or highs[i - 2] >= closes[i - 8]
    cond3 = lambda i, lows, highs: lows[i] <= highs[i - 5] or lows[i] <= highs[i - 6]
    cond4 = lambda i, lows, closes: lows[i - 2] <= closes[i - 7] or lows[i - 2] <= closes[i - 8]

    def calc_rei(series: pd.Series) -> float:
        highs = df.loc[series.index, Column.HIGH]
        lows = df.loc[series.index, Column.LOW]
        closes = df.loc[series.index, Column.CLOSE]

        s1 = 0
        s2 = 0
        for i in range(period):
            start_index = -1 - i
            end_index = start_index - 2
            high_diff = highs[start_index] - highs[end_index]
            low_diff = lows[start_index] - lows[end_index]
            if (cond1(start_index, highs, lows) or cond2(start_index, highs, closes)) and (cond3(start_index, lows, highs) or cond4(start_index, lows, closes)):
                s1 += high_diff + low_diff
            s2 += abs(high_diff) + abs(low_diff)

        rei_val = 100 * (s1 / s2)

        return rei_val

    def calc_poq(series: pd.Series) -> str:
        opens = df.loc[series.index, Column.OPEN]
        highs = df.loc[series.index, Column.HIGH]
        lows = df.loc[series.index, Column.LOW]
        closes = df.loc[series.index, Column.CLOSE]
        rei = df.loc[series.index, IndicatorRegistry.TD_RANGE_EXPANSION_INDEX.value]
        poq = 1

        buy_cond1 = rei[-1] < -40 <= rei[-7]
        buy_cond2 = closes[-2] < closes[-3]
        buy_cond3 = opens[-1] <= highs[-2] and opens[-1] <= highs[-3]
        buy_cond4 = opens[-1] < highs[-1] and (highs[-1] > min(highs[-2], highs[-3]))

        sell_cond1 = rei[-1] > 40 >= rei[-7]
        sell_cond2 = closes[-2] > closes[-3]
        sell_cond3 = opens[-1] >= lows[-2] and opens[-1] >= lows[-3]
        sell_cond4 = opens[-1] > lows[-1] and (lows[-1] < max(lows[-2], lows[-3]))

        if buy_cond1 and buy_cond2 and buy_cond3 and buy_cond4:
            poq = 2
        if sell_cond1 and sell_cond2 and sell_cond3 and sell_cond4:
            poq = 3
        return poq

    df[IndicatorRegistry.TD_RANGE_EXPANSION_INDEX.value] = df[Column.HIGH].rolling(period + 8).apply(calc_rei, raw=False)
    df[IndicatorRegistry.TD_POQ.value] = df[Column.HIGH].rolling(period + 8).apply(calc_poq, raw=False)
    df[IndicatorRegistry.TD_POQ.value] = df[IndicatorRegistry.TD_POQ.value].apply(lambda v: "NA" if np.isnan(v) or v == 1.0 else "BUY" if v == 2.0 else "SELL")
    return price_history
