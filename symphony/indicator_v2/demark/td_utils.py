from symphony.data_classes import PriceHistory
from symphony.indicator_v2 import IndicatorRegistry
from symphony.exceptions import IndicatorException
from typing import List, NewType
from symphony.config import USE_MODIN
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

MarketOrderType = NewType('Indicator', IndicatorRegistry)


def get_start_ts(price_history: PriceHistory, start: pd.Timestamp) -> pd.Timestamp:
    """
    Helper to get starting timestamp. Performs some sanity checks.
    If none supplied, then start at beginning of DataFrame

    :param price_history:
    :param start:
    :return: (`pd.Timestamp`)
    :raises IndicatorException: If the start timestamp is out of range.
    """
    df = price_history.price_history
    if start:
        if start > df.index[-1]:
            raise IndicatorException(f"Start index {str(start)} later than latest date {str(df.index[-1])}")
        if start < df.index[0]:
            raise IndicatorException(f"Start index {str(start)} earlier than earliest date {str(df.index[0])}")
        start_ts = start
    else:
        start_ts = df.index[0]
    return start_ts


def combine_pattern_start_index(price_history: PriceHistory, new_pattern_start_index: pd.Series) -> PriceHistory:
    """
    Merges pattern start index Series into the dataframe. IndicatorRegistry.PATTERN_START_INDEX column holds
    all start (integer) indices of patterns. These should never overlap, so they can be merged.

    :param price_history: Standard PriceHistory
    :param new_pattern_start_index: The index to merge
    :return: price_history
    """
    df = price_history.price_history
    if len(df) != len(new_pattern_start_index):
        raise IndicatorException(f"Error combining pattern start indices. "
                                 f"Mismatched lengths. len(df): {len(df)}, len(index): {len(new_pattern_start_index)}")
    if IndicatorRegistry.PATTERN_START_INDEX.value not in df.columns:
        df[IndicatorRegistry.PATTERN_START_INDEX.value] = new_pattern_start_index.values
    else:
        df[IndicatorRegistry.PATTERN_START_INDEX.value] = \
            df[IndicatorRegistry.PATTERN_START_INDEX.value].combine(
                new_pattern_start_index, lambda val1, val2: max(val1, val2) if val1 or val2 else 0).values

    return price_history


def __assert_columns_present(price_history: PriceHistory, indicator: IndicatorRegistry) -> None:
    """
    Identifies missing columns

    :param price_history: (`PriceHistory`) Standard price history
    :param indicator: (`IndicatorRegistry.INDICATOR`) Indicatory to check
    :return: (`None`)
    :raises IndicatorException: If columns are missing or the indicator is unrecognized
    """
    df = price_history.price_history
    if indicator == IndicatorRegistry.BUY_SETUP:
        if IndicatorRegistry.BEARISH_PRICE_FLIP.value not in df.columns:
            raise IndicatorException(f"No {IndicatorRegistry.BEARISH_PRICE_FLIP.value} present")

    elif indicator == IndicatorRegistry.SELL_SETUP:
        if IndicatorRegistry.BULLISH_PRICE_FLIP.value not in df.columns:
            raise IndicatorException(f"No {IndicatorRegistry.BULLISH_PRICE_FLIP.value} present")

    elif indicator == IndicatorRegistry.BUY_COUNTDOWN or indicator == IndicatorRegistry.SELL_COUNTDOWN \
            or indicator == IndicatorRegistry.BUY_COMBO or indicator == IndicatorRegistry.SELL_COMBO:
        if IndicatorRegistry.BEARISH_PRICE_FLIP.value not in df.columns \
                or IndicatorRegistry.BULLISH_PRICE_FLIP.value not in df.columns \
                or IndicatorRegistry.SELL_SETUP.value not in df.columns \
                or IndicatorRegistry.BUY_SETUP.value not in df.columns:
            raise IndicatorException(f"Missing required columns to calculate countdown. Columns {df.columns}")

    elif indicator == IndicatorRegistry.BUY_9_13_9 or indicator == IndicatorRegistry.SELL_9_13_9:
        if IndicatorRegistry.BEARISH_PRICE_FLIP.value not in df.columns \
                or IndicatorRegistry.BULLISH_PRICE_FLIP.value not in df.columns \
                or IndicatorRegistry.SELL_SETUP.value not in df.columns \
                or IndicatorRegistry.BUY_SETUP.value not in df.columns:
            raise IndicatorException(f"Missing required columns to calculate countdown. Columns {df.columns}")
        if indicator == IndicatorRegistry.BUY_9_13_9:
            if IndicatorRegistry.BUY_COUNTDOWN.value not in df.columns:
                raise IndicatorException(f"Missing Buy Countdown column. Columns {df.columns}")
        if indicator == IndicatorRegistry.SELL_9_13_9:
            if IndicatorRegistry.SELL_COUNTDOWN.value not in df.columns:
                raise IndicatorException(f"Missing Sell Countdown column. Columns {df.columns}")
    else:
        raise IndicatorException(f"Unknown indicator: {indicator}")
    return
