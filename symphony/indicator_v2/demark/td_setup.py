from symphony.indicator_v2 import IndicatorRegistry, IndicatorKit
from symphony.data_classes import PriceHistory, copy_price_history
from symphony.exceptions import IndicatorException
from symphony.enum import Column
from .td_utils import __assert_columns_present, get_start_ts
from symphony.utils.time import filter_start
import numpy as np
from typing import List, Union, Optional
from symphony.config import USE_MODIN
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


def td_buy_setup(price_history: PriceHistory, start: Optional[Union[pd.Timestamp, None]] = None,
                 price_history_copy: Optional[bool] = False, max_bars: Optional[int] = -1) -> PriceHistory:
    """
    Calculates the BUY Setups, Perfect BUY Setups, TDST Resistance, and True BUY Setup Range.
    Modifies in place and returns price history.

    :param price_history: (`data_classes.PriceHistory`) Price history object
    :param start: (`pd.Timestamp`) Optional index to start at
    :param price_history_copy: (`bool`) Return a deep copy of price history
    :return: (`data_classes.PriceHistory`) The price history object with all indicators appended
    :raises IndicatorException: If there are no price flips present
    """

    __assert_columns_present(price_history, IndicatorRegistry.BUY_SETUP)
    # Optionally copy without modifying in place
    if price_history_copy:
        price_history = copy_price_history(price_history)
        df = price_history.price_history
    else:
        df = price_history.price_history

    if max_bars != -1 and not isinstance(start, type(None)):
        raise IndicatorException(f"start and max_bars cannot both be defined. Start: {start}, Max Bars: {max_bars}")


    if start or max_bars == -1 or max_bars >= len(df):
        start_ts = get_start_ts(price_history, start)
    elif max_bars > 0 and max_bars < len(df):
        df = df.iloc[-max_bars:]
        start_ts = df.index[0]

    price_flip_indices = df.index[df[IndicatorRegistry.BEARISH_PRICE_FLIP.value] == 1].tolist()
    price_flip_indices = filter_start(price_flip_indices, start_ts)

    setups = pd.Series(np.zeros(len(df), dtype="int32"))
    perfect_setups = pd.Series(np.zeros(len(df), dtype="int32"))
    tdst_resistance = pd.Series(np.zeros(len(df), dtype="int32"))

    index: pd.Timestamp
    for index in price_flip_indices:
        integer_index = df.index.get_loc(index)
        for i in range(integer_index, integer_index + 9):
            # Out of bounds check
            if i >= len(df):
                break
            if df[Column.CLOSE].iloc[i] < df[Column.CLOSE].iloc[i - 4]:
                if i == integer_index + 8:
                    setups[i] = 1
                    tdst_resistance[i:] = IndicatorKit.get_true_high(price_history, integer_index)
                    # Check if perfect
                    # The low of bars eight or nine of the TD Buy Setup or a subsequent low must be less
                    # than, or equal to, the lows of bars six and seven of the TD Buy Setup.
                    if (df[Column.LOW].iloc[i] <= df[Column.LOW].iloc[i - 2] and df[Column.LOW].iloc[i] <=
                        df[Column.LOW].iloc[i - 3]) or \
                            (df[Column.LOW].iloc[i - 1] <= df[Column.LOW].iloc[i - 2] and df[Column.LOW].iloc[i - 1] <=
                             df[Column.LOW].iloc[
                                 i - 3]):
                        perfect_setups[i] = 1
            else:
                break

    df[IndicatorRegistry.BUY_SETUP.value] = setups.values
    df[IndicatorRegistry.PERFECT_BUY_SETUP.value] = perfect_setups.values
    df[IndicatorRegistry.TDST_RESISTANCE.value] = tdst_resistance.values

    setup_true_end_indices = pd.Series(np.zeros(len(df), dtype="int32"))
    setup_indices: List[pd.Timestamp] = df.index[df[IndicatorRegistry.BUY_SETUP.value] == 1].tolist()

    for timestamp_setup_index in setup_indices:
        integer_index = df.index.get_loc(timestamp_setup_index)
        for i in range(integer_index + 1, len(df)):
            if df[Column.CLOSE].iloc[i] < df[Column.CLOSE].iloc[i - 4]:
                # Handle case where we are at the most recent bar
                if i == len(df) - 1:
                    setup_true_end_indices[integer_index] = i
                continue
            else:
                # TODO: Switch to timestamp?
                setup_true_end_indices[integer_index] = i - 1
                break

    df[IndicatorRegistry.BUY_SETUP_TRUE_END_INDEX.value] = setup_true_end_indices.values

    return price_history


def td_sell_setup(price_history: PriceHistory, start: pd.Timestamp = None,
                  price_history_copy: bool = False, max_bars: Optional[int] = -1) -> PriceHistory:
    """
        Calculates the SELL Setups, Perfect SELL Setups, TDST Support, and True SELL Setup Range
        Modifies in place and returns price history.

        :param price_history: (`data_classes.PriceHistory`) Price history object
        :param start: (`pd.Timestamp`) Optional index to start at
        :param price_history_copy: (`bool`) Return a deep copy of price history
        :return: (`data_classes.PriceHistory`) The price history object with all indicators appended
        :raises IndicatorException: If there are no price flips present
        """

    __assert_columns_present(price_history, IndicatorRegistry.SELL_SETUP)
    # Optionally copy without modifying in place
    if price_history_copy:
        price_history = copy_price_history(price_history)
        df = price_history.price_history
    else:
        df = price_history.price_history

    start_ts = get_start_ts(price_history, start)

    price_flip_indices = df.index[df[IndicatorRegistry.BULLISH_PRICE_FLIP.value] == 1].tolist()
    price_flip_indices = filter_start(price_flip_indices, start_ts)

    setups = pd.Series(np.zeros(len(df), dtype="int32"))
    perfect_setups = pd.Series(np.zeros(len(df), dtype="int32"))
    tdst_support = pd.Series(np.zeros(len(df), dtype="int32"))

    index: pd.Timestamp
    for index in price_flip_indices:
        integer_index = df.index.get_loc(index)
        for i in range(integer_index, integer_index + 9):
            # Out of bounds check
            if i >= len(df):
                break
            if df[Column.CLOSE].iloc[i] > df[Column.CLOSE].iloc[i - 4]:
                if i == integer_index + 8:
                    setups[i] = 1
                    tdst_support[i:] = IndicatorKit.get_true_low(price_history, integer_index)
                    # Check if perfect
                    # The low of bars eight or nine of the TD Buy Setup or a subsequent low must be less
                    # than, or equal to, the lows of bars six and seven of the TD Buy Setup.
                    if (df[Column.HIGH].iloc[i] <= df[Column.HIGH].iloc[i - 2] and df[Column.HIGH].iloc[i] <= df[Column.HIGH].iloc[
                        i - 3]) or \
                            (df[Column.HIGH].iloc[i - 1] <= df[Column.HIGH].iloc[i - 2] and df[Column.HIGH].iloc[i - 1] <=
                             df[Column.HIGH].iloc[
                                 i - 3]):
                        perfect_setups[i] = 1
            else:
                break

    df[IndicatorRegistry.SELL_SETUP.value] = setups.values
    df[IndicatorRegistry.PERFECT_SELL_SETUP.value] = perfect_setups.values
    df[IndicatorRegistry.TDST_SUPPORT.value] = tdst_support.values

    setup_true_end_indices = pd.Series(np.zeros(len(df), dtype="int32"))
    setup_indices: List[pd.Timestamp] = df.index[df[IndicatorRegistry.SELL_SETUP.value] == 1].tolist()
    for timestamp_setup_index in setup_indices:
        integer_index = df.index.get_loc(timestamp_setup_index)

        for i in range(integer_index + 1, len(df)):
            if df[Column.CLOSE].iloc[i] > df[Column.CLOSE].iloc[i - 4]:
                # Handle case where we are at the most recent bar
                if i == len(df) - 1:
                    setup_true_end_indices[integer_index] = i
                continue
            else:
                setup_true_end_indices[integer_index] = i - 1
                break

    df[IndicatorRegistry.SELL_SETUP_TRUE_END_INDEX.value] = setup_true_end_indices.values

    return price_history
