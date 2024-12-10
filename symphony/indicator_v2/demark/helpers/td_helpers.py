from symphony.data_classes import PriceHistory
from symphony.indicator_v2 import IndicatorRegistry, IndicatorKit
from symphony.exceptions import IndicatorException
from symphony.enum import Column
from symphony.utils import standardize_index
from ..td_dwave import WaveConstants
from typing import List, NewType, Union, Optional
import pandas as pd

PatternType = NewType('PatternType', IndicatorRegistry)


def td_stoploss(price_history: PriceHistory,
                pattern_type: PatternType,
                pattern_index: Union[pd.Timestamp, int]) -> float:
    """
    Calculates the risk level (stoploss) for a given pattern identified by index

    :param price_history: Standard price history
    :param pattern_type: IndicatorRegistry.TD_PATTERN
    :param pattern_index: Either int or Timestamp index of the pattern
    :return: The stoploss
    :raises IndicatorException: If the pattern is not in the dataframe, if the pattern
        index is the wrong type, if instrument's digits cannot be determined, if the
        pattern type is unrecognized
    """
    df = price_history.price_history
    if pattern_type.value not in df.columns:
        raise IndicatorException(f"This pattern {pattern_type.value} is not in the dataframe")

    # Use integer index
    index = standardize_index(price_history, pattern_index)

    # Get pattern start index
    if pattern_type == IndicatorRegistry.BUY_SETUP or pattern_type == IndicatorRegistry.SELL_SETUP:
        pattern_start_index: int = index - 8
    else:
        pattern_start_index: int = df[IndicatorRegistry.PATTERN_START_INDEX.value].iloc[index]

    # Pattern should never start at 0. Even if there is a setup at the very beginning of the frame, it
    #   will be preceded by a six bar price flip
    if pattern_start_index <= 0:
        raise IndicatorException(f"Could not find pattern start index. Index: {pattern_start_index}")

    # Calculate risk level
    if pattern_type == IndicatorRegistry.BUY_SETUP or pattern_type == IndicatorRegistry.BUY_COUNTDOWN \
            or pattern_type == IndicatorRegistry.AGGRESSIVE_BUY_COUNTDOWN or pattern_type == IndicatorRegistry.BUY_COMBO \
            or pattern_type == IndicatorRegistry.BUY_9_13_9:
        true_lows: List[float] = [IndicatorKit.get_true_low(price_history, i) for i in
                                  range(pattern_start_index, index + 1)]
        lowest_true_low: float = min(true_lows)
        lowest_true_low_index: int = pattern_start_index + true_lows.index(lowest_true_low)
        bar_true_range: float = IndicatorKit.get_true_range(price_history, lowest_true_low_index)

        if type(price_history.instrument.digits) != int:
            raise IndicatorException(f"Could not determine digits {price_history.instrument.digits} "
                                     f"for symbol {price_history.instrument.symbol}")

        stop_loss = round(lowest_true_low - bar_true_range, price_history.instrument.digits)
        return max(0.0, stop_loss)

    elif pattern_type == IndicatorRegistry.SELL_SETUP or pattern_type == IndicatorRegistry.SELL_COUNTDOWN \
            or pattern_type == IndicatorRegistry.AGGRESSIVE_SELL_COUNTDOWN or pattern_type == IndicatorRegistry.SELL_COMBO \
            or pattern_type == IndicatorRegistry.SELL_9_13_9:
        true_highs: List[float] = [IndicatorKit.get_true_high(price_history, i) for i in
                                   range(pattern_start_index, index + 1)]
        highest_true_high: float = max(true_highs)
        highest_true_high_index: int = pattern_start_index + true_highs.index(highest_true_high)
        bar_true_range: float = IndicatorKit.get_true_range(price_history, highest_true_high_index)

        if type(price_history.instrument.digits) != int:
            raise IndicatorException(f"Could not determine digits {price_history.instrument.digits} "
                                     f"for symbol {price_history.instrument.symbol}")

        return round(
            highest_true_high + bar_true_range, price_history.instrument.digits
        )
    else:
        raise IndicatorException(f"Unknown pattern type: {pattern_type}, value {pattern_type.value}")


def is_overbought(price_history: PriceHistory, indicator: IndicatorKit, index: Optional[int] = -1) -> bool:
    """
    Returns True if overbought for various indicators. Currently TD Demarker I & II, TD Pressure.

    :param price_history: Standard price history
    :param indicator: One of IndicatorKit.TD_DEMARKER_I
    :param index: Optional index
    :return:
    """
    df = price_history.price_history
    if len(df) < 14:
        return False

    if indicator.value not in df.columns:
        raise IndicatorException(f"{indicator} Is not in columns {df.columns}")

    if indicator == IndicatorRegistry.TD_DEMARKER_I:
        cond1 = df[IndicatorRegistry.TD_DEMARKER_I.value].iloc[index] <= 0.4 < df[IndicatorRegistry.TD_DEMARKER_I.value].iloc[index - 13]
        cond2 = df[Column.CLOSE].iloc[index] < max(df[Column.LOW].iloc[index - 1], df[Column.LOW].iloc[index - 2])
        cond3 = df[Column.CLOSE].iloc[index] < min(df[Column.CLOSE].iloc[index - 1], df[Column.OPEN].iloc[index - 1])
        cond4 = df[Column.CLOSE].iloc[index] <= max(df[Column.OPEN].iloc[index], df[Column.CLOSE].iloc[index - 1])
        if cond1 and cond2 and cond3 and cond4:
            return True
    elif indicator == IndicatorRegistry.TD_DEMARKER_II:
        return df[IndicatorRegistry.TD_DEMARKER_II.value].iloc[index] > 0.6
    elif indicator == IndicatorRegistry.TD_PRESSURE:
        if df[IndicatorRegistry.TD_PRESSURE.value].iloc[index] > 0.75:
            return True
    else:
        raise IndicatorException(f"Not implemented for {indicator}")
    return False


def is_oversold(price_history: PriceHistory, indicator: IndicatorKit, index: Optional[int] = -1) -> bool:
    """
    Returns True if oversold for various indicators. Currently TD Demarker I & II, TD Pressure.

    :param price_history: Standard price history
    :param indicator: One of IndicatorKit.TD_DEMARKER_I
    :param index: Optional index
    :return:
    """
    df = price_history.price_history
    if len(df) < 14:
        return False

    if indicator.value not in df.columns:
        raise IndicatorException(f"{indicator} Is not in colums {df.columns}")

    if indicator == IndicatorRegistry.TD_DEMARKER_I:
        cond1 = df[IndicatorRegistry.TD_DEMARKER_I.value].iloc[index] >= 0.6 > df[IndicatorRegistry.TD_DEMARKER_I.value].iloc[index - 6]
        cond2 = df[Column.CLOSE].iloc[index] > min(df[Column.HIGH].iloc[index - 1], df[Column.HIGH].iloc[index - 2])
        cond3 = df[Column.CLOSE].iloc[index] > max(df[Column.CLOSE].iloc[index - 1], df[Column.OPEN].iloc[index - 1])
        cond4 = df[Column.CLOSE].iloc[index] >= min(df[Column.OPEN].iloc[index], df[Column.CLOSE].iloc[index - 1])
        if cond1 and cond2 and cond3 and cond4:
            return True
    elif indicator == IndicatorRegistry.TD_DEMARKER_II:
        return df[IndicatorRegistry.TD_DEMARKER_I.value].iloc[index] < 0.4
    elif indicator == IndicatorRegistry.TD_PRESSURE:
        if df[IndicatorRegistry.TD_PRESSURE.value].iloc[index] < 0.25:
            return True
    else:
        raise IndicatorException(f"Not implemented for {indicator}")
    return False

def get_td_wave_rep(wave: WaveConstants) -> str:
    """
    Get string representation of TD DWave

    :param wave: Wave constant
    :return: string
    :raises IndicatorException: If the wave is unknown
    """

    if wave == WaveConstants.WAVE_0:
        return "WAVE_0"
    elif wave == WaveConstants.WAVE_1:
        return "WAVE_1"
    elif wave == WaveConstants.WAVE_1C1:
        return "WAVE_1C1"
    elif wave == WaveConstants.WAVE_1C2:
        return "WAVE_1C2"
    elif wave == WaveConstants.WAVE_2:
        return "WAVE_2"
    elif wave == WaveConstants.WAVE_3:
        return "WAVE_3"
    elif wave == WaveConstants.WAVE_4:
        return "WAVE_4"
    elif wave == WaveConstants.WAVE_5:
        return "WAVE_5"
    elif wave == WaveConstants.WAVE_A:
        return "WAVE_A"
    elif wave == WaveConstants.WAVE_B:
        return "WAVE_B"
    elif wave == WaveConstants.WAVE_C:
        return "WAVE_C"
    else:
        raise IndicatorException(f"Unknown wave: {wave}")


def get_string_rep_short(indicator: IndicatorRegistry) -> str:
    """
    Gets a shortened string representation of a Demark indicator. Used for embedding in order ids.

    :param indicator: Indicator
    :return: Short string representation
    :raises IndicatorException: If the indicator is unknown
    """
    if indicator == IndicatorRegistry.TDST_RESISTANCE:
        return "tdstr"
    elif indicator == IndicatorRegistry.TDST_SUPPORT:
        return "tdsts"
    elif indicator == IndicatorRegistry.BULLISH_PRICE_FLIP:
        return "bupf"
    elif indicator == IndicatorRegistry.BEARISH_PRICE_FLIP:
        return "bepf"
    elif indicator == IndicatorRegistry.BUY_SETUP:
        return "bs"
    elif indicator == IndicatorRegistry.SELL_SETUP:
        return "ss"
    elif indicator == IndicatorRegistry.PERFECT_BUY_SETUP:
        return "pbs"
    elif indicator == IndicatorRegistry.PERFECT_SELL_SETUP:
        return "pss"
    elif indicator == IndicatorRegistry.BUY_COUNTDOWN:
        return "bcd"
    elif indicator == IndicatorRegistry.SELL_COUNTDOWN:
        return "scd"
    elif indicator == IndicatorRegistry.BUY_COMBO:
        return "bcom"
    elif indicator == IndicatorRegistry.SELL_COMBO:
        return "scom"
    elif indicator == IndicatorRegistry.AGGRESSIVE_BUY_COUNTDOWN:
        return "abc"
    elif indicator == IndicatorRegistry.AGGRESSIVE_SELL_COUNTDOWN:
        return "asc"
    elif indicator == IndicatorRegistry.BUY_9_13_9:
        return "b9139"
    elif indicator == IndicatorRegistry.SELL_9_13_9:
        return "s9139"
    else:
        raise IndicatorException(f"Unknown indicator: {indicator}")


def get_indicator_from_short_string_rep(string_rep: str) -> str:
    """
    Gets the indicator from a shortened string representation

    :param string_rep: String representation used in the order ids
    :return: The actual indicator name
    :raises IndicatorException: If the string representation is unknown
    """
    if string_rep == "tdstr":
        return IndicatorRegistry.TDST_RESISTANCE
    elif string_rep == "tdsts":
        return IndicatorRegistry.TDST_SUPPORT
    elif string_rep == "bupf":
        return IndicatorRegistry.BULLISH_PRICE_FLIP
    elif string_rep == "bepf":
        return IndicatorRegistry.BEARISH_PRICE_FLIP
    elif string_rep == "bs":
        return IndicatorRegistry.BUY_SETUP
    elif string_rep == "ss":
        return IndicatorRegistry.SELL_SETUP
    elif string_rep == "pbs":
        return IndicatorRegistry.PERFECT_BUY_SETUP
    elif string_rep == "pss":
        return IndicatorRegistry.PERFECT_SELL_SETUP
    elif string_rep == "bcd":
        return IndicatorRegistry.BUY_COUNTDOWN
    elif string_rep == "scd":
        return IndicatorRegistry.SELL_COUNTDOWN
    elif string_rep == "bcom":
        return IndicatorRegistry.BUY_COMBO
    elif string_rep == "scom":
        return IndicatorRegistry.SELL_COMBO
    elif string_rep == "abc":
        return IndicatorRegistry.AGGRESSIVE_BUY_COUNTDOWN
    elif string_rep == "asc":
        return IndicatorRegistry.AGGRESSIVE_SELL_COUNTDOWN
    elif string_rep == "b9139":
        return IndicatorRegistry.BUY_9_13_9
    elif string_rep == "s9139":
        return IndicatorRegistry.SELL_9_13_9
    else:
        raise IndicatorException(f"Unknown indicator: {string_rep}")