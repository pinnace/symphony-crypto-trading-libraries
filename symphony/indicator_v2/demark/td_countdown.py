from symphony.indicator_v2 import IndicatorRegistry, IndicatorKit
from symphony.data_classes import PriceHistory, copy_price_history
from symphony.exceptions import IndicatorException
from symphony.config import LOG_LEVEL, USE_MODIN
from .td_utils import __assert_columns_present, get_start_ts, combine_pattern_start_index
from symphony.utils.time import filter_start
import numpy as np
from symphony.enum import Column
from typing import List, Union, NewType
import logging
from symphony.utils import glh
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

ColumnType = NewType('Column', Column)
SetupType = NewType('SetupType', IndicatorRegistry)
logger = logging.getLogger(__name__)


def td_buy_countdown(price_history: PriceHistory,
                     start: Union[pd.Timestamp, None] = None,
                     price_history_copy: bool = False,
                     cancellation_qualifier_I: bool = False,
                     cancellation_qualifier_II: bool = False,
                     log_level: logging = LOG_LEVEL
                     ) -> PriceHistory:
    """
    Performs calculations for TD Buy Countdown and Aggressive Buy Countdown.
    Addresses:
        1. Cancellation Qualifier I
        2. Cancellation Qualifier II
        3. General cancellation qualifiers
        4. Countdown Recycle Qualifier
        5. Aggressive countdowns

    :param price_history: (`PriceHistory`) Standard price history
    :param start: (`Union[pd.Timestamp, None]`) Optional starting point for calculation
    :param price_history_copy: (`bool`) Optionally return deep copy of price history
    :param cancellation_qualifier_I: (`bool`) Whether to use CCI
    :param cancellation_qualifier_II: (`bool`) Whether to use CCII
    :param log_level: (`logging.LEVEL`) Specify logging level
    :return: (`PriceHistory`) Either the original or copied price history
    :raises IndicatorException: If start index is out of bounds
    """

    logger.setLevel(log_level)
    __assert_columns_present(price_history, IndicatorRegistry.BUY_COUNTDOWN)

    if price_history_copy:
        price_history = copy_price_history(price_history)
        df = price_history.price_history
    else:
        df = price_history.price_history

    start_ts = get_start_ts(price_history, start)

    buy_setup_indices: List[pd.Timestamp] = df.index[df[IndicatorRegistry.BUY_SETUP.value] == 1].tolist()
    buy_setup_indices = filter_start(buy_setup_indices, start_ts)

    if cancellation_qualifier_I | cancellation_qualifier_II:
        active_buy_setups = pd.Series(np.zeros(len(df), dtype="int32"))

        if cancellation_qualifier_I:
            active_buy_setups = __cancellation_qualifier_I(price_history, IndicatorRegistry.BUY_SETUP,
                                                           buy_setup_indices,
                                                           active_buy_setups)
        if cancellation_qualifier_II:
            active_buy_setups = __cancellation_qualifier_II(price_history, IndicatorRegistry.BUY_SETUP,
                                                            buy_setup_indices,
                                                            active_buy_setups)
        buy_setup_indices: List[pd.Timestamp] = [
            df.index[index] for index in
            active_buy_setups.index[active_buy_setups == 1].tolist()
        ]

    # TODO: test what happens if indices len is 0
    buy_countdowns = pd.Series(np.zeros(len(df), dtype="int32"))
    buy_countdown_pattern_start_indices = pd.Series(np.zeros(len(df), dtype="int32"))
    buy_countdown_pattern_start_indices.index = df.index
    aggressive_buy_countdowns = pd.Series(np.zeros(len(df), dtype="int32"))
    sell_setups: List[pd.Timestamp] = df.index[df[IndicatorRegistry.SELL_SETUP.value] == 1].tolist()

    # Core countdown calculation
    buy_setup_index: pd.Timestamp
    for buy_setup_index in buy_setup_indices:
        buy_setup_integer_index: int = df.index.get_loc(buy_setup_index)
        count: int = 0
        aggressive_count: int = 0
        aggressive_found: bool = False
        bar8_close: float = 0.0
        for i in range(buy_setup_integer_index, len(df)):
            # Cancel buy countdown if there is a true low over TDST Resistance
            if IndicatorKit.get_true_low(price_history, i) > df[IndicatorRegistry.TDST_RESISTANCE.value].iloc[i]:
                logger.info(
                    f"{glh(price_history)}[{IndicatorRegistry.BUY_COUNTDOWN.value.upper()}][!]"
                    f" Countdown at {buy_setup_index} cancelled because low over TDST Resistance")
                break
            # Cancel countdown if market generates a sell setup
            if df.index[i] in sell_setups:
                logger.info(
                    f"{glh(price_history)}[{IndicatorRegistry.BUY_COUNTDOWN.value.upper()}][!]"
                    f" Countdown at {buy_setup_index} cancelled because sell setup appeared")
                break
            # Recycle countdown if buy setup extends 18 or more bars.
            buy_setup_true_end_integer_index: int = df[IndicatorRegistry.BUY_SETUP_TRUE_END_INDEX.value].iloc[
                buy_setup_integer_index]
            if buy_setup_true_end_integer_index - buy_setup_integer_index >= 9:
                logger.info(
                    f"{glh(price_history)}[{IndicatorRegistry.BUY_COUNTDOWN.value.upper()}][!]"
                    f" Countdown starting at setup {buy_setup_index} recycled at {df.index[buy_setup_true_end_integer_index]} "
                    f"because setup extended 18 or more bars")
                break

            # Countdown
            if df[Column.CLOSE].iloc[i] <= df[Column.LOW].iloc[i - 2]:
                if count < 12:
                    count += 1
                elif count == 12 and df[Column.LOW].iloc[i] < bar8_close:
                    logger.info(
                        f"{glh(price_history)}[{IndicatorRegistry.BUY_COUNTDOWN.value.upper()}][+]"
                        f" Found BUY countdown at {str(df.index[i])} for setup at {str(buy_setup_index)}")
                    buy_countdowns.iloc[i] = 1
                    buy_countdown_pattern_start_indices.iloc[i] = buy_setup_integer_index - 8
                    break
            if not bar8_close and count == 8:
                bar8_close = df[Column.CLOSE].iloc[i]

            # Aggressive Countdown
            if df[Column.LOW].iloc[i] <= df[Column.LOW].iloc[i - 2]:
                if aggressive_count < 12:
                    aggressive_count += 1
                elif aggressive_count == 12 and not aggressive_found:
                    logger.info(
                        f"{glh(price_history)}[{IndicatorRegistry.AGGRESSIVE_BUY_COUNTDOWN.value.upper()}][+]"
                        f" Found Aggressive BUY countdown at {str(df.index[i])} for setup at {str(buy_setup_index)}")
                    aggressive_found = True
                    aggressive_buy_countdowns.iloc[i] = 1
                    buy_countdown_pattern_start_indices.iloc[i] = buy_setup_integer_index - 8

    df[IndicatorRegistry.BUY_COUNTDOWN.value] = buy_countdowns.values
    df[IndicatorRegistry.AGGRESSIVE_BUY_COUNTDOWN.value] = aggressive_buy_countdowns.values
    price_history = combine_pattern_start_index(price_history, buy_countdown_pattern_start_indices)

    return price_history


def td_sell_countdown(price_history: PriceHistory,
                      start: Union[pd.Timestamp, None] = None,
                      price_history_copy: bool = False,
                      cancellation_qualifier_I: bool = False,
                      cancellation_qualifier_II: bool = False,
                      log_level: logging = LOG_LEVEL
                      ) -> PriceHistory:
    """
    Performs calculations for TD Sell Countdown and Aggressive Sell Countdown.
    Addresses:
        1. Cancellation Qualifier I
        2. Cancellation Qualifier II
        3. General cancellation qualifiers
        4. Countdown Recycle Qualifier

    :param price_history: (`PriceHistory`) Standard price history
    :param start: (`Union[pd.Timestamp, None]`) Optional starting point for calculation
    :param price_history_copy: (`bool`) Optionally return deep copy of price history
    :param cancellation_qualifier_I: (`bool`) Whether to use CCI
    :param cancellation_qualifier_II: (`bool`) Whether to use CCII
    :param log_level: (`logging.LEVEL`) Specify logging level
    :return: (`PriceHistory`) Either the original or copied price history
    """
    logger.setLevel(log_level)
    __assert_columns_present(price_history, IndicatorRegistry.SELL_COUNTDOWN)
    if price_history_copy:
        price_history = copy_price_history(price_history)
        df = price_history.price_history
    else:
        df = price_history.price_history

    start_ts = get_start_ts(price_history, start)

    sell_setup_indices = df.index[df[IndicatorRegistry.SELL_SETUP.value] == 1].tolist()
    sell_setup_indices = filter_start(sell_setup_indices, start_ts)

    if cancellation_qualifier_I | cancellation_qualifier_II:
        active_sell_setups = pd.Series(np.zeros(len(df), dtype="int32"))

        if cancellation_qualifier_I:
            active_sell_setups = __cancellation_qualifier_I(price_history, IndicatorRegistry.SELL_SETUP,
                                                            sell_setup_indices,
                                                            active_sell_setups)
        if cancellation_qualifier_II:
            active_sell_setups = __cancellation_qualifier_II(price_history, IndicatorRegistry.SELL_SETUP,
                                                             sell_setup_indices,
                                                             active_sell_setups)
        sell_setup_indices: List[pd.Timestamp] = [
            df.index[index] for index in
            active_sell_setups.index[active_sell_setups == 1].tolist()
        ]

    # TODO: test what happens if indices len is 0
    sell_countdowns = pd.Series(np.zeros(len(df), dtype="int32"))
    sell_countdown_pattern_start_indices = pd.Series(np.zeros(len(df), dtype="int32"))
    sell_countdown_pattern_start_indices.index = df.index
    aggressive_sell_countdowns = pd.Series(np.zeros(len(df), dtype="int32"))
    buy_setups: List[pd.Timestamp] = df.index[df[IndicatorRegistry.BUY_SETUP.value] == 1].tolist()

    sell_setup_index: pd.Timestamp
    for sell_setup_index in sell_setup_indices:
        sell_setup_integer_index: int = df.index.get_loc(sell_setup_index)
        count: int = 0
        aggressive_count: int = 0
        aggressive_found: bool = False
        bar8_close: float = 0.0
        for i in range(sell_setup_integer_index, len(df)):
            # Cancel sell countdown if there is a true high over TDST Support
            if IndicatorKit.get_true_high(price_history, i) < df[IndicatorRegistry.TDST_SUPPORT.value].iloc[i]:
                logger.info(
                    f"{glh(price_history)}[{IndicatorRegistry.SELL_COUNTDOWN.value.upper()}][!]"
                    f" Countdown at {sell_setup_index} cancelled because high over TDST Support")
                break
            # Cancel countdown if market generates a buy setup
            if df.index[i] in buy_setups:
                logger.info(
                    f"{glh(price_history)}[{IndicatorRegistry.SELL_COUNTDOWN.value.upper()}][!]"
                    f" Countdown at {sell_setup_index} cancelled because buy setup appeared")
                break
            # Recycle countdown if sell setup extends 18 or more bars.
            sell_setup_true_end_integer_index: int = df[IndicatorRegistry.SELL_SETUP_TRUE_END_INDEX.value].iloc[
                sell_setup_integer_index]
            if sell_setup_true_end_integer_index - sell_setup_integer_index >= 9:
                logger.info(
                    f"{glh(price_history)}[{IndicatorRegistry.SELL_COUNTDOWN.value.upper()}][!]"
                    f" Countdown starting at setup {sell_setup_index} recycled at {df.index[sell_setup_true_end_integer_index]} "
                    f"because setup extended 18 or more bars")
                break

            # Countdown
            if df[Column.CLOSE].iloc[i] >= df[Column.HIGH].iloc[i - 2]:
                if count < 12:
                    count += 1
                elif count == 12 and df[Column.HIGH].iloc[i] > bar8_close:
                    logger.info(
                        f"{glh(price_history)}[{IndicatorRegistry.SELL_COUNTDOWN.value.upper()}][+]"
                        f" Found SELL countdown at {str(df.index[i])} for setup at {str(sell_setup_index)}")
                    sell_countdowns.iloc[i] = 1
                    sell_countdown_pattern_start_indices.iloc[i] = sell_setup_integer_index - 8
                    break
            if not bar8_close and count == 8:
                bar8_close = df[Column.CLOSE].iloc[i]

            # Aggressive Countdown
            if df[Column.HIGH].iloc[i] >= df[Column.HIGH].iloc[i - 2]:
                if aggressive_count < 12:
                    aggressive_count += 1
                elif aggressive_count == 12 and not aggressive_found:
                    logger.info(
                        f"{glh(price_history)}[{IndicatorRegistry.AGGRESSIVE_SELL_COUNTDOWN.value.upper()}][+]"
                        f" Found Aggressive SELL countdown at {str(df.index[i])} for setup at {str(sell_setup_index)}")
                    aggressive_found = True
                    aggressive_sell_countdowns.iloc[i] = 1
                    sell_countdown_pattern_start_indices.iloc[i] = sell_setup_integer_index - 8

    df[IndicatorRegistry.SELL_COUNTDOWN.value] = sell_countdowns.values
    df[IndicatorRegistry.AGGRESSIVE_SELL_COUNTDOWN.value] = aggressive_sell_countdowns.values
    price_history = combine_pattern_start_index(price_history, sell_countdown_pattern_start_indices)

    return price_history


def __setup_true_range(
        price_history: PriceHistory,
        setup_index: pd.Timestamp,
        setup_type: SetupType
) -> float:
    """
    Helper method to get a setup's true range

    :param price_history: (`PriceHistory`) Price history object
    :param setup_index: (`pd.Timestamp`) Timestamp index of bar 9
    :param setup_type: (`Union[IndicatorRegistry.BUY_SETUP, IndicatorRegistry.SELL_SETUP]`) type of index
    :return: (`float`) True range
    :raises IndicatorException: If the `setup_type` is unknown
    """

    df = price_history.price_history
    setup_integer_index: int = df.index.get_loc(setup_index)
    setup_start_integer_index: int = setup_integer_index - 8
    if setup_type == IndicatorRegistry.BUY_SETUP:
        setup_true_end_integer_index: int = df[IndicatorRegistry.BUY_SETUP_TRUE_END_INDEX.value].iloc[
            setup_integer_index]
    elif setup_type == IndicatorRegistry.SELL_SETUP:
        setup_true_end_integer_index: int = df[IndicatorRegistry.SELL_SETUP_TRUE_END_INDEX.value].iloc[
            setup_integer_index]
    else:
        raise IndicatorException(f"Unknown setup type {setup_type}")
    setup_true_range = IndicatorKit.get_true_range_of_interval(price_history, setup_start_integer_index,
                                                   setup_true_end_integer_index)
    return setup_true_range


def __max_of(price_history: PriceHistory,
             setup_index: pd.Timestamp,
             setup_type: SetupType,
             point: ColumnType
             ) -> float:
    """
    Get max value of a pattern. Either high or close. Handles finding true setup end.

    :param price_history: (`PriceHistory`) Standard price history
    :param setup_index: (`pd.Timestamp`) Index of bar 9
    :param setup_type: (`IndicatoryRegistry`) Either BUY or SELL setup
    :param point: (`ColumnType`) Column.HIGH or Column.CLOSE
    :return: (`float`) The maximum point
    :raises IndicatorException: If an incorrect Column or pattern was supplied
    """
    if point != Column.CLOSE and point != Column.HIGH:
        raise IndicatorException(f"Improper point for __max_of: {point}")

    df = price_history.price_history
    start_integer_index: int = df.index.get_loc(setup_index) - 8

    if setup_type == IndicatorRegistry.BUY_SETUP:
        setup_true_end_integer_index: int = df[IndicatorRegistry.BUY_SETUP_TRUE_END_INDEX.value].loc[setup_index]
    elif setup_type == IndicatorRegistry.SELL_SETUP:
        setup_true_end_integer_index: int = df[IndicatorRegistry.SELL_SETUP_TRUE_END_INDEX.value].loc[setup_index]
    else:
        raise IndicatorException(f"Unknown indicator: {setup_type}")

    if point == Column.CLOSE:
        return max(
            df[Column.CLOSE].iloc[start_integer_index:setup_true_end_integer_index]
        )
    if point == Column.HIGH:
        return max(
            [
                IndicatorKit.get_true_high(price_history, i) for i in
                range(start_integer_index, setup_true_end_integer_index + 1)
            ]
        )


def __min_of(
        price_history: PriceHistory,
        setup_index: pd.Timestamp,
        setup_type: SetupType,
        point: ColumnType
) -> float:
    """
    Get min value of a pattern. Either low or close. Handles finding true setup end.

    :param price_history: (`PriceHistory`) Standard price history
    :param setup_index: (`pd.Timestamp`) Index of bar 9
    :param setup_type: (`IndicatoryRegistry`) Either BUY or SELL setup
    :param point: (`ColumnType`) Column.LOW or Column.CLOSE
    :return: (`float`) The minimum point
    :raises IndicatorException: If an incorrect Column or pattern was supplied
    """
    if point != Column.CLOSE and point != Column.LOW:
        raise IndicatorException(f"Improper point for __min_of: {point}")

    df = price_history.price_history
    start_integer_index: int = df.index.get_loc(setup_index) - 8

    if setup_type == IndicatorRegistry.BUY_SETUP:
        setup_true_end_integer_index: int = df[IndicatorRegistry.BUY_SETUP_TRUE_END_INDEX.value].loc[setup_index]
    elif setup_type == IndicatorRegistry.SELL_SETUP:
        setup_true_end_integer_index: int = df[IndicatorRegistry.SELL_SETUP_TRUE_END_INDEX.value].loc[setup_index]
    else:
        raise IndicatorException(f"Unknown indicator: {setup_type}")

    if point == Column.CLOSE:
        return min(
            df[Column.CLOSE].iloc[start_integer_index:setup_true_end_integer_index]
        )
    if point == Column.LOW:
        return min(
            [
                IndicatorKit.get_true_low(price_history, i) for i in
                range(start_integer_index, setup_true_end_integer_index + 1)
            ]
        )


def __cancellation_qualifier_I(
        price_history: PriceHistory,
        setup_type: SetupType,
        target_setup_indices: List[pd.Timestamp],
        active_setup_indices: pd.Series
) -> pd.Series:
    """
    Calculates CCI. Modifies active_setup_indices inplace and returns it also

    TD Buy Countdown Cancellation Qualifier I
    If
        The size of the true range of the most recently completed TD Buy Setup is equal to,
        or greater than, the size of the previous TD Buy Setup, but less than 1.618 times its size,
    Then
        A TD Setup recycle will occur; that is, whichever TD Buy Setup has the larger true
        range will become the active TD Buy Setup.
    :param price_history: (`PriceHistory`) Price History object
    :param setup_type: (`SetupType`) either BUY_SETUP or SELL_SETUP
    :param target_setup_indices: (`List[pd.TimeStamp]`) List of buy or sell setup indices
    :param active_setup_indices: (`pd.Series`) The pandas series where we keep the list of active setups
    :return: (`pd.Series`)
    :raises IndicatorException: If setup type cannot be identified
    """
    if setup_type != IndicatorRegistry.BUY_SETUP and setup_type != IndicatorRegistry.SELL_SETUP:
        raise IndicatorException(f"Could not identify setup type for CCI: {setup_type}")

    # If there are no setups, do nothing
    if not len(target_setup_indices):
        return active_setup_indices

    df = price_history.price_history
    # If only one setup, this is the only active one
    if len(target_setup_indices) == 1:
        integer_index = df.index.get_loc(target_setup_indices[0])
        active_setup_indices.iloc[integer_index] = 1
        return active_setup_indices

    if len(target_setup_indices) > 1:
        for pre_setup_int_index, setup_index in enumerate(target_setup_indices[1:]):
            setup_integer_index: int = df.index.get_loc(setup_index)
            curr_true_range: float = __setup_true_range(price_history, setup_index, setup_type)

            # Note that `pre_setup_int_index` actually 1 index back in `target_setup_indices`
            prev_setup_index: pd.Timestamp = target_setup_indices[pre_setup_int_index]
            prev_setup_integer_index: int = df.index.get_loc(prev_setup_index)
            prev_true_range: float = __setup_true_range(price_history, prev_setup_index, setup_type)

            if prev_true_range <= curr_true_range <= 1.618 * prev_true_range:
                if curr_true_range >= prev_true_range:
                    logger.info(
                        f"{glh(price_history)}[{setup_type.value.upper()}][!] CCI Fulfilled: Set Current Setup {setup_index} as active")
                    active_setup_indices.iloc[setup_integer_index] = 1
                    active_setup_indices.iloc[prev_setup_integer_index] = 0
                else:
                    logger.info(
                        f"{glh(price_history)}[{setup_type.value.upper()}][!] CCI Fulfilled: Set Previous Setup {prev_setup_index} as active")
                    active_setup_indices.iloc[setup_integer_index] = 0
                    active_setup_indices.iloc[prev_setup_integer_index] = 1
            else:
                logger.info(
                    f"{glh(price_history)}[{setup_type.value.upper()}][+] CCI Unfulfilled: Keeping current {setup_index} and previous {prev_setup_index} setup as active")
                active_setup_indices.iloc[setup_integer_index] = 1
                active_setup_indices.iloc[prev_setup_integer_index] = 1

    return active_setup_indices


def __cancellation_qualifier_II(
        price_history: PriceHistory,
        setup_type: SetupType,
        target_setup_indices: List[pd.Timestamp],
        active_setup_indices: pd.Series
) -> pd.Series:
    """
    Evaluates CCII
    TD Buy Countdown Cancellation Qualifier II (a TD Buy Setup Within a TD	Buy	Setup)
    If
        The market has completed a TD Buy Setup that has a closing range within the true
        range of the prior TD Buy Setup, without recording a TD Sell Setup between the two,
    And if
        The current TD Buy Setup has a price extreme within the true range of the prior TD Buy Setup,
    Then
        The prior TD Buy Setup is the active TD Setup, and the TD Buy Countdown relating to it remains intact.


    :param price_history: (`PriceHistory`) Price History object
    :param setup_type: (`SetupType`) either BUY_SETUP or SELL_SETUP
    :param target_setup_indices: (`List[pd.TimeStamp]`) List of buy or sell setup indices
    :param active_setup_indices: (`pd.Series`) The pandas series where we keep the list of active setups
    :return: (`pd.Series`)
    :raises IndicatorException: If setup type cannot be identified
    """
    if setup_type != IndicatorRegistry.BUY_SETUP and setup_type != IndicatorRegistry.SELL_SETUP:
        raise IndicatorException(f"Could not identify setup type for CCII: {setup_type}")

    # If there are no setups, do nothing
    if not len(target_setup_indices):
        return active_setup_indices

    df = price_history.price_history
    # If only one setup, this is the only active one
    if len(target_setup_indices) == 1:
        integer_index = df.index.get_loc(target_setup_indices[0])
        active_setup_indices.iloc[integer_index] = 1
        return active_setup_indices

    if len(target_setup_indices) > 1:
        if setup_type == IndicatorRegistry.BUY_SETUP:
            opposite_side_setups: List[pd.Timestamp] = df.index[df[IndicatorRegistry.SELL_SETUP.value] == 1].tolist()
        else:
            opposite_side_setups: List[pd.Timestamp] = df.index[df[IndicatorRegistry.BUY_SETUP.value] == 1].tolist()

        for pre_setup_int_index, setup_index in enumerate(target_setup_indices[1:]):
            prev_setup_index: pd.Timestamp = target_setup_indices[pre_setup_int_index]
            # Ensure there are none of the opposite side Setups between the two
            if len(opposite_side_setups):
                if list(filter(lambda opp_side_setup_ts: setup_index > opp_side_setup_ts > prev_setup_index,
                               opposite_side_setups)):
                    logger.debug(f"[{setup_type.value.upper()}] Found setup inbetween.")
                    continue

            prev_true_high = __max_of(price_history, prev_setup_index, setup_type, Column.HIGH)
            prev_true_low = __min_of(price_history, prev_setup_index, setup_type, Column.LOW)

            curr_highest_high = __max_of(price_history, setup_index, setup_type, Column.HIGH)
            curr_highest_close = __max_of(price_history, setup_index, setup_type, Column.CLOSE)
            curr_lowest_low = __min_of(price_history, setup_index, setup_type, Column.LOW)
            curr_lowest_close = __min_of(price_history, setup_index, setup_type, Column.CLOSE)

            prev_setup_integer_index = df.index.get_loc(prev_setup_index)
            curr_setup_integer_index = df.index.get_loc(setup_index)

            if curr_highest_high < prev_true_high and curr_highest_close < prev_true_high \
                    and curr_lowest_low > prev_true_low and curr_lowest_close > prev_true_low:
                logger.info(
                    f"{glh(price_history)}[{setup_type.value.upper()}][!] CCII Fulfilled: Set Previous Setup {prev_setup_index} as active")
                active_setup_indices.iloc[prev_setup_integer_index] = 1
                active_setup_indices.iloc[curr_setup_integer_index] = 0
            else:
                logger.info(
                    f"{glh(price_history)}[{setup_type.value.upper()}][+] CCII Unfulfilled: Keeping both setups as active")
                active_setup_indices.iloc[prev_setup_integer_index] = 1
                active_setup_indices.iloc[curr_setup_integer_index] = 1

    return active_setup_indices
