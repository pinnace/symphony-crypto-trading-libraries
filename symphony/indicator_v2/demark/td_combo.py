from symphony.indicator_v2 import IndicatorRegistry
from symphony.data_classes import PriceHistory, copy_price_history
from symphony.config import LOG_LEVEL, USE_MODIN
from .td_utils import __assert_columns_present, get_start_ts, combine_pattern_start_index
from symphony.utils.time import filter_start
import numpy as np
from symphony.enum import Column
from typing import List, Union
from symphony.utils import glh
import logging
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

logger = logging.getLogger(__name__)


def td_buy_combo(price_history: PriceHistory,
                 strict: bool = True,
                 start: Union[pd.Timestamp, None] = None,
                 price_history_copy: bool = False,
                 log_level: logging = LOG_LEVEL
                 ) -> PriceHistory:
    """
    Calculates the TD Buy Combo according to the following conditions:
        1. The close must be less than, or equal to, the low two price bars earlier;
        2. Each TD Combo Buy Countdownlow must be less than, or equal to, the low of the
        prior price bar;
        3. Each TD Combo Buy Countdown close must be less than the previous TD Combo
        Buy Countdown close; and
        4. Each TD Combo Buy Countdown close must be less than the close of the prior
        price bar

    :param price_history: Standard price history
    :param strict: Whether to use strict or less-strict version
    :param start: Optional start date
    :param price_history_copy: Whether or not to return a copy or original price history
    :param log_level: Optionally specify an alternative log level
    :return: price_history
    """
    logger.setLevel(log_level)
    __assert_columns_present(price_history, IndicatorRegistry.BUY_COMBO)

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

    combo_buys = pd.Series(np.zeros(len(df), dtype="int32"))
    combo_buy_start_indices = pd.Series(np.zeros(len(df), dtype="int32"))
    combo_buy_start_indices.index = df.index

    for buy_setup_index in buy_setup_indices:
        buy_setup_start_integer_index: int = df.index.get_loc(buy_setup_index) - 8
        count = 1
        prev_combo_close = df[Column.CLOSE].iloc[buy_setup_start_integer_index]
        for i in range(buy_setup_start_integer_index, len(df)):
            bar_index: pd.Timestamp = df.index[i]
            if bar_index in sell_setup_indices:
                logger.debug(
                    f"{glh(price_history)}[{IndicatorRegistry.BUY_COMBO.value.upper()}][+] buy combo cancelled:")
                break

            elif not strict and (10 <= count < 13):
                if df[Column.CLOSE].iloc[i] < df[Column.CLOSE].iloc[i - 1]:
                    count += 1
            elif (strict and count < 13) or (not strict and count < 10):
                if df[Column.CLOSE].iloc[i] <= df[Column.LOW].iloc[i - 2] and \
                        df[Column.LOW].iloc[i] <= df[Column.LOW].iloc[i - 1] and \
                        df[Column.CLOSE].iloc[i] < df[Column.CLOSE].iloc[i - 1] and \
                        df[Column.CLOSE].iloc[i] < prev_combo_close:
                    prev_combo_close = df[Column.CLOSE].iloc[i]
                    count += 1
            if count == 13:
                logger.info(
                    f"{glh(price_history)}[{IndicatorRegistry.BUY_COMBO.value.upper()}][+]"
                    f" Found {'strict' if strict else 'less-strict'} Combo BUY at {str(df.index[i])} for setup at {str(buy_setup_index)}")
                combo_buys.iloc[i] = 1
                combo_buy_start_indices.iloc[i] = buy_setup_start_integer_index
                break

    df[IndicatorRegistry.BUY_COMBO.value] = combo_buys.values
    price_history = combine_pattern_start_index(price_history, combo_buy_start_indices)
    return price_history


def td_sell_combo(price_history: PriceHistory,
                  strict: bool = True,
                  start: Union[pd.Timestamp, None] = None,
                  price_history_copy: bool = False,
                  log_level: logging = LOG_LEVEL
                  ) -> PriceHistory:
    """
    Calculates the Sell Combo according to
        1.	 The close must be greater than, or equal to, the high two price bars earlier;
        2. Each TD Combo Sell Countdown high must be greater than, or equal to, the high
            of the previous price bar;
        3. Each TD Combo Sell Countdown close must be greater than the close of the
            previous TD Combo Sell Countdown; and
        4. Each TD Combo Sell Countdown close must be greater than the close of the
            previous price bar.

    :param price_history: Standard price history
    :param strict: Whether to use strict or less-strict version
    :param start: Optional start date
    :param price_history_copy: Whether or not to return a copy or original price history
    :param log_level: Optionally specify an alternative log level
    :return: price_history
    """
    logger.setLevel(log_level)
    __assert_columns_present(price_history, IndicatorRegistry.SELL_COMBO)

    if price_history_copy:
        price_history = copy_price_history(price_history)
        df = price_history.price_history
    else:
        df = price_history.price_history

    start_ts = get_start_ts(price_history, start)
    sell_setup_indices = df.index[df[IndicatorRegistry.SELL_SETUP.value] == 1].tolist()
    sell_setup_indices = filter_start(sell_setup_indices, start_ts)
    buy_setup_indices: List[pd.Timestamp] = df.index[df[IndicatorRegistry.BUY_SETUP.value] == 1].tolist()
    buy_setup_indices = filter_start(buy_setup_indices, start_ts)

    combo_sells = pd.Series(np.zeros(len(df), dtype="int32"))
    combo_sell_start_indices = pd.Series(np.zeros(len(df), dtype="int32"))
    combo_sell_start_indices.index = df.index
    for sell_setup_index in sell_setup_indices:
        sell_setup_start_integer_index: int = df.index.get_loc(sell_setup_index) - 8
        count = 1
        prev_combo_close = df[Column.CLOSE].iloc[sell_setup_start_integer_index]
        for i in range(sell_setup_start_integer_index, len(df)):
            bar_index: pd.Timestamp = df.index[i]
            if bar_index in buy_setup_indices:
                logger.debug(
                    f"{glh(price_history)}[{IndicatorRegistry.SELL_COMBO.value.upper()}][+] sell combo cancelled")
                break

            elif not strict and (10 <= count < 13):
                if df[Column.CLOSE].iloc[i] > df[Column.CLOSE].iloc[i - 1]:
                    count += 1
            elif (strict and count < 13) or (not strict and count < 10):
                if df[Column.CLOSE].iloc[i] >= df[Column.HIGH].iloc[i - 2] and \
                        df[Column.HIGH].iloc[i] >= df[Column.HIGH].iloc[i - 1] and \
                        df[Column.CLOSE].iloc[i] > df[Column.CLOSE].iloc[i - 1] and \
                        df[Column.CLOSE].iloc[i] > prev_combo_close:
                    prev_combo_close = df[Column.CLOSE].iloc[i]
                    count += 1

            if count == 13:
                logger.info(
                    f"{glh(price_history)}[{IndicatorRegistry.SELL_COMBO.value.upper()}][+]"
                    f" Found {'strict' if strict else 'less-strict'} Combo SELL at {str(df.index[i])} for setup at {str(sell_setup_index)}")
                combo_sells.iloc[i] = 1
                combo_sell_start_indices.iloc[i] = sell_setup_start_integer_index
                break

    df[IndicatorRegistry.SELL_COMBO.value] = combo_sells.values
    price_history = combine_pattern_start_index(price_history, combo_sell_start_indices)

    return price_history
