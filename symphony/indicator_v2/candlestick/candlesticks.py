from symphony.data_classes import PriceHistory
from symphony.config import USE_MODIN
import numpy as np
from typing import Optional
from .helpers.rankings import candle_rankings
import pandas_ta as ta

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


def candlesticks(price_history: PriceHistory, doji_name: Optional[str] = "CDL_DOJI_10_0.1") -> PriceHistory:
    """
    Calculates candlestick patterns. Warning: This is a slow function.
    Sets column "candlestick_pattern" to the highest ranked pattern found, or "NO_PATTERN" if none found.
    Sets column "candlestick_pattern_direction" to "BUY", "SELL", or "NA".
    Rankings are in candlesticks.helpers.rankings

    :param price_history: Supplied price history
    :param doji_name: The name for the pandas_ta DOJI column
    :return: Price history with "CANDLESTICK_PATTERN" and "CANDLESTICK_PATTERN_DIRECTION"
    """

    price_history.price_history['candlestick_pattern'] = np.nan
    price_history.price_history['candlestick_pattern_direction'] = np.nan
    candlesticks: pd.DataFrame = price_history.price_history.ta.cdl_pattern(name="all")
    cols = candlesticks.columns
    for index, row in candlesticks.iterrows():
        patterns_filter = row.apply(lambda x: x != 0.0)
        present_patterns = row[patterns_filter.values]
        if "CDL_INSIDE" in present_patterns:
            present_patterns = present_patterns.drop("CDL_INSIDE")

        if not len(present_patterns):
            pattern_name = "NO_PATTERN"
            pattern_direction = "NA"
        if len(present_patterns) == 1:
            pattern_name = list(present_patterns.index)[0]
            if present_patterns[pattern_name] > 0.0:
                pattern_direction = "BUY"
            else:
                pattern_direction = "SELL"

        if len(present_patterns) > 1:
            ranking = 1000
            top_pattern = ""
            top_pattern_direction = ""
            for pattern in present_patterns.index:
                pattern_norm = __normalize_pattern(pattern, doji_name)

                if present_patterns[pattern] >= 100.0:
                    direction = "BUY"
                    dir_pattern = pattern_norm + "_Bull"
                elif present_patterns[pattern] <= -100.0:
                    direction = "SELL"
                    dir_pattern = pattern_norm + "_Bear"
                else:
                    print(f"Unknown direction")
                    breakpoint()
                if candle_rankings[dir_pattern] < ranking:
                    top_pattern = pattern_norm
                    top_pattern_direction = direction
                    ranking = candle_rankings[dir_pattern]
            pattern_name = top_pattern
            pattern_direction = top_pattern_direction

        pattern_name = __normalize_pattern(pattern_name, doji_name)
        price_history.price_history.loc[index, "candlestick_pattern"] = pattern_name
        price_history.price_history.loc[index, "candlestick_pattern_direction"] = pattern_direction

    return price_history


def __normalize_pattern(name: str, doji_name: str) -> str:
    if name == doji_name:
        return "CDL_DOJI"
    return name
