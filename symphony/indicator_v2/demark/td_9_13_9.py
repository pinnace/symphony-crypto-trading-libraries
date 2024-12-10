from symphony.indicator_v2 import IndicatorRegistry
from symphony.data_classes import PriceHistory, copy_price_history
from symphony.config import LOG_LEVEL, USE_MODIN
from .td_utils import __assert_columns_present, get_start_ts, combine_pattern_start_index
from symphony.utils.time import filter_start
import numpy as np
from symphony.enum import Market
from typing import List, Union, NewType
import logging
from symphony.utils import glh
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

logger = logging.getLogger(__name__)
MarketOrderType = NewType('Market', Market)


def td_buy_9_13_9(price_history: PriceHistory,
                  start: Union[pd.Timestamp, None] = None,
                  price_history_copy: bool = False,
                  log_level: logging = LOG_LEVEL
                  ) -> PriceHistory:
    """
    Identifies valid 9-13-9 BUY signals.
    Requirements:
        1. There must be a BULLISH price flip inbetween end of countdown and
            beginning of buy setup
        2. The Buy setup must not begin before bar 13 of the countdown
        3. There must not be a sell setup inbetween the countdown and setup

    :param price_history: (`PriceHistory`) Standard price history
    :param start: (`pd.Timestamp`) Optional start index
    :param price_history_copy: (`bool`) Optionally return new price history
    :param log_level: (`logging.LEVEL`) Optionally override default log level
    :return: (`PriceHistory`) New or copied
    """
    logger.setLevel(log_level)
    __assert_columns_present(price_history, IndicatorRegistry.BUY_9_13_9)

    if price_history_copy:
        price_history = copy_price_history(price_history)
        df = price_history.price_history
    else:
        df = price_history.price_history

    start_ts = get_start_ts(price_history, start)

    buy_setup_indices: List[pd.Timestamp] = df.index[df[IndicatorRegistry.BUY_SETUP.value] == 1].tolist()
    buy_setup_indices = filter_start(buy_setup_indices, start_ts)
    sell_setup_indices: List[pd.Timestamp] = df.index[df[IndicatorRegistry.SELL_SETUP.value] == 1].tolist()
    sell_setup_indices = filter_start(sell_setup_indices, start_ts)
    bullish_price_flip_indices: List[pd.Timestamp] = df.index[
        df[IndicatorRegistry.BULLISH_PRICE_FLIP.value] == 1].tolist()
    bullish_price_flip_indices = filter_start(bullish_price_flip_indices, start_ts)
    buy_countdown_indices: List[pd.Timestamp] = df.index[df[IndicatorRegistry.BUY_COUNTDOWN.value] == 1].tolist()
    buy_countdown_indices = filter_start(buy_countdown_indices, start_ts)

    buy_9_13_9s = pd.Series(np.zeros(len(df), dtype="int32"))
    buy_9_13_9_start_indices = pd.Series(np.zeros(len(df), dtype="int32"))
    buy_9_13_9_start_indices.index = df.index

    for countdown_index in buy_countdown_indices:
        countdown_int_index: int = df.index.get_loc(countdown_index)
        must_start_after_integer_index = countdown_int_index + 9
        # Check for out of bounds
        if must_start_after_integer_index >= len(df):
            continue

        must_start_after_index = df.index[must_start_after_integer_index]
        buy_setups_after: List[pd.Timestamp] = sorted(
            list(filter(lambda buy_setup_index: buy_setup_index >= must_start_after_index, buy_setup_indices)))
        if len(buy_setups_after):
            sell_setups_after: List[pd.Timestamp] = sorted(
                list(filter(lambda sell_setup_index: sell_setup_index >= countdown_index, sell_setup_indices)))
            if len(sell_setups_after) and sell_setups_after[0] < buy_setups_after[0]:
                continue
            buy_setup_start_int_index: int = df.index.get_loc(buy_setups_after[0]) - 9
            buy_setup_start: pd.Timestamp = df.index[buy_setup_start_int_index]
            bullish_price_flips_inbetween: List[pd.Timestamp] = \
                sorted(
                    list(
                        filter(
                            lambda
                                bullish_price_flip_index: bullish_price_flip_index >= countdown_index and bullish_price_flip_index < buy_setup_start,
                            bullish_price_flip_indices
                        )))
            if len(bullish_price_flips_inbetween) == 1:
                logger.info(
                    f"{glh(price_history)}[{IndicatorRegistry.BUY_9_13_9.value.upper()}][+] Bullish 9-13-9 found at {buy_setups_after[0]} ")
                buy_setup_int_index: int = df.index.get_loc(buy_setups_after[0])
                buy_9_13_9s[buy_setup_int_index] = 1
                buy_9_13_9_start_indices.iloc[buy_setup_int_index] = df[IndicatorRegistry.PATTERN_START_INDEX.value].loc[countdown_index]

    df[IndicatorRegistry.BUY_9_13_9.value] = buy_9_13_9s.values
    price_history = combine_pattern_start_index(price_history, buy_9_13_9_start_indices)
    return price_history


def td_sell_9_13_9(price_history: PriceHistory,
                   start: Union[pd.Timestamp, None] = None,
                   price_history_copy: bool = False,
                   log_level: logging = LOG_LEVEL
                   ) -> PriceHistory:
    """
    Identifies valid 9-13-9 SELL signals.
    Requirements:
        1. There must be a BEARISH price flip inbetween end of countdown and
            beginning of sell setup
        2. The Sell setup must not begin before bar 13 of the countdown
        3. There must not be a buy setup inbetween the countdown and setup

    :param price_history: (`PriceHistory`) Standard price history
    :param start: (`pd.Timestamp`) Optional start index
    :param price_history_copy: (`bool`) Optionally return new price history
    :param log_level: (`logging.LEVEL`) Optionally override default log level
    :return: (`PriceHistory`) New or copied
    """
    logger.setLevel(log_level)
    __assert_columns_present(price_history, IndicatorRegistry.SELL_9_13_9)

    if price_history_copy:
        price_history = copy_price_history(price_history)
        df = price_history.price_history
    else:
        df = price_history.price_history

    start_ts = get_start_ts(price_history, start)

    buy_setup_indices: List[pd.Timestamp] = df.index[df[IndicatorRegistry.BUY_SETUP.value] == 1].tolist()
    buy_setup_indices = filter_start(buy_setup_indices, start_ts)
    sell_setup_indices: List[pd.Timestamp] = df.index[df[IndicatorRegistry.SELL_SETUP.value] == 1].tolist()
    sell_setup_indices = filter_start(sell_setup_indices, start_ts)
    bearish_price_flip_indices: List[pd.Timestamp] = df.index[
        df[IndicatorRegistry.BEARISH_PRICE_FLIP.value] == 1].tolist()
    bearish_price_flip_indices = filter_start(bearish_price_flip_indices, start_ts)
    sell_countdown_indices: List[pd.Timestamp] = df.index[df[IndicatorRegistry.SELL_COUNTDOWN.value] == 1].tolist()
    sell_countdown_indices = filter_start(sell_countdown_indices, start_ts)

    sell_9_13_9s = pd.Series(np.zeros(len(df), dtype="int32"))
    sell_9_13_9_start_indices = pd.Series(np.zeros(len(df), dtype="int32"))
    sell_9_13_9_start_indices.index = df.index
    for countdown_index in sell_countdown_indices:
        countdown_int_index: int = df.index.get_loc(countdown_index)
        must_start_after_integer_index = countdown_int_index + 9
        # Check for out of bounds
        if must_start_after_integer_index >= len(df):
            continue
        must_start_after_index = df.index[must_start_after_integer_index]
        sell_setups_after: List[pd.Timestamp] = sorted(
            list(filter(lambda sell_setup_index: sell_setup_index >= must_start_after_index, sell_setup_indices)))
        if len(sell_setups_after):
            buy_setups_after: List[pd.Timestamp] = sorted(
                list(filter(lambda buy_setup_index: buy_setup_index >= countdown_index, buy_setup_indices)))
            if len(buy_setups_after) and buy_setups_after[0] < sell_setups_after[0]:
                continue
            sell_setup_start_int_index: int = df.index.get_loc(sell_setups_after[0]) - 9
            sell_setup_start: pd.Timestamp = df.index[sell_setup_start_int_index]
            bearish_price_flips_inbetween: List[pd.Timestamp] = \
                sorted(
                    list(
                        filter(
                            lambda
                                bearish_price_flip_index: bearish_price_flip_index >= countdown_index and bearish_price_flip_index < sell_setup_start,
                            bearish_price_flip_indices
                        )))
            if len(bearish_price_flips_inbetween) == 1:
                logger.info(
                    f"{glh(price_history)}[{IndicatorRegistry.SELL_9_13_9.value.upper()}][+] Bearish 9-13-9 found at {sell_setups_after[0]} ")
                sell_setup_int_index: int = df.index.get_loc(sell_setups_after[0])
                sell_9_13_9s[sell_setup_int_index] = 1
                sell_9_13_9_start_indices.iloc[sell_setup_int_index] = df[IndicatorRegistry.PATTERN_START_INDEX.value].loc[countdown_index]

    df[IndicatorRegistry.SELL_9_13_9.value] = sell_9_13_9s.values
    price_history = combine_pattern_start_index(price_history, sell_9_13_9_start_indices)
    return price_history
