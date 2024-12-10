import numpy as np

from symphony.config import USE_MODIN
from symphony.data_classes import PriceHistory
from symphony.enum import Column
from enum import Enum, auto
from symphony.indicator_v2 import IndicatorRegistry
from symphony.exceptions import IndicatorException
from symphony.utils.time import standardize_index
from typing import Optional, Union

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


def zig_zag(
        price_history: PriceHistory,
        percent: Optional[float] = 0.08,
        length: Optional[int] = 10,
        include_repaints: Optional[bool] = True,
        with_harmonics: Optional[bool] = True,
        harmonics_error_rate: Optional[float] = 0.05
) -> PriceHistory:
    """
    Computes the ZigZag indicator with optional harmonics.
    Harmonics supported:
    GARTLEY, CYPHER, BAT, BUTTERFLY, CRAB, CYPHER, ALTBAT, DEEPCRAB

    :param price_history: Standard price history
    :param percent: Deviation %
    :param length: ZigZag minimum pattern length
    :param include_repaints: Optionally include a series ZIGZAG_REPAINTS. Will override if repaints already in series.
    :param with_harmonics: Optionally calculate Harmonics
    :param harmonics_error_rate: The deviation allowed with harmonic patterns
    :return: Price history with indicator
    """

    df = price_history.price_history

    if IndicatorRegistry.ZIGZAG_REPAINT.value in df.columns:
        include_repaints = True

    # Find starting point if ZigZags already present
    first_found = False
    if IndicatorRegistry.ZIGZAG.value in df.columns:

        if len(df[df[IndicatorRegistry.ZIGZAG.value] == 1.0]):
            zz_high_most_recent = df[df[IndicatorRegistry.ZIGZAG.value] == 1.0].index[-1]
            zz_high = df[Column.HIGH].loc[zz_high_most_recent]
            zz_high_most_recent_int = df.index.get_loc(zz_high_most_recent)

            zz_low_most_recent = df[df[IndicatorRegistry.ZIGZAG.value] == -1.0].index[-1]
            zz_low = df[Column.LOW].loc[zz_low_most_recent]
            zz_low_most_recent_int = df.index.get_loc(zz_low_most_recent)

            if zz_high_most_recent_int > zz_low_most_recent_int:
                zz_curr_index = zz_high_most_recent_int
                trend = "UP"
            else:
                zz_curr_index = zz_low_most_recent_int
                trend = "DOWN"
            first_found = True

    # Seed values if first call
    if not first_found or IndicatorRegistry.ZIGZAG.value not in df.columns:
        df[IndicatorRegistry.ZIGZAG.value] = 0.0
        if include_repaints:
            df[IndicatorRegistry.ZIGZAG_REPAINT.value] = 0.0
        seed_low = min(df[Column.LOW].iloc[:length])
        seed_high = max(df[Column.HIGH].iloc[:length])
        seed_low_index = df[Column.LOW].iloc[:length].tolist().index(seed_low)
        seed_high_index = df[Column.HIGH].iloc[:length].tolist().index(seed_high)
        first_found = False

        zz_high = None
        zz_low = None
        zz_curr_index = 0

        trend = ""

    # Set up Harmonics columns if not present
    if with_harmonics and IndicatorRegistry.HARMONIC.value not in df.columns:
        df[IndicatorRegistry.HARMONIC.value] = 0

    for i in range(length, len(df)):
        curr_high = df[Column.HIGH].iloc[i]
        curr_low = df[Column.LOW].iloc[i]
        if not first_found:
            if seed_high_index != i and curr_high >= seed_high:
                seed_high_index = i
                seed_high = curr_high
            if seed_low_index != i and curr_low <= seed_low:
                seed_low_index = i
                seed_low = curr_low
            if (curr_high / seed_low) - 1.0 >= percent:
                first_found = True
                trend = "UP"
                zz_high = curr_high
                zz_curr_index = i
                zz_low = seed_low
                df[IndicatorRegistry.ZIGZAG.value].iloc[seed_low_index] = -1.0
                df[IndicatorRegistry.ZIGZAG.value].iloc[zz_curr_index] = 1.0
            elif 1.0 - (curr_low / seed_high) >= percent:
                first_found = True
                trend = "DOWN"
                zz_high = seed_high
                zz_curr_index = i
                zz_low = curr_low
                df[IndicatorRegistry.ZIGZAG.value].iloc[seed_high_index] = 1.0
                df[IndicatorRegistry.ZIGZAG.value].iloc[zz_curr_index] = -1.0

        else:

            # If in uptrend and new high made, repaint
            if trend == "UP" and zz_curr_index != i and curr_high >= zz_high:
                df[IndicatorRegistry.ZIGZAG.value].iloc[zz_curr_index] = 0.0
                if include_repaints:
                    df[IndicatorRegistry.ZIGZAG_REPAINT.value].iloc[zz_curr_index] = 1.0
                zz_curr_index = i
                df[IndicatorRegistry.ZIGZAG.value].iloc[zz_curr_index] = 1.0
                zz_high = curr_high
                if with_harmonics:
                    df[IndicatorRegistry.HARMONIC.value].iloc[zz_curr_index] = __identify_harmonic_pattern(price_history, error_rate=harmonics_error_rate)


            # If downtrend and new low made, repaint
            elif trend == "DOWN" and zz_curr_index != i and curr_low <= zz_low:
                df[IndicatorRegistry.ZIGZAG.value].iloc[zz_curr_index] = 0.0
                if include_repaints:
                    df[IndicatorRegistry.ZIGZAG_REPAINT.value].iloc[zz_curr_index] = -1.0
                zz_curr_index = i
                df[IndicatorRegistry.ZIGZAG.value].iloc[zz_curr_index] = -1.0
                zz_low = curr_low
                if with_harmonics:
                    df[IndicatorRegistry.HARMONIC.value].iloc[zz_curr_index] = __identify_harmonic_pattern(price_history, error_rate=harmonics_error_rate)

            # Skip this round if pattern is not yet of sufficient length
            if i - zz_curr_index < length:
                continue

            if trend == "UP" and 1.0 - (curr_low / zz_high) >= percent:
                trend = "DOWN"
                zz_curr_index = i
                df[IndicatorRegistry.ZIGZAG.value].iloc[zz_curr_index] = -1.0
                zz_low = curr_low
                if with_harmonics:
                    df[IndicatorRegistry.HARMONIC.value].iloc[zz_curr_index] = __identify_harmonic_pattern(price_history, error_rate=harmonics_error_rate)

            elif trend == "DOWN" and (curr_high / zz_low) - 1.0 >= percent:
                trend = "UP"
                zz_curr_index = i
                df[IndicatorRegistry.ZIGZAG.value].iloc[zz_curr_index] = 1.0
                zz_high = curr_high
                if with_harmonics:
                    df[IndicatorRegistry.HARMONIC.value].iloc[zz_curr_index] = __identify_harmonic_pattern(price_history, error_rate=harmonics_error_rate)

    return price_history


class PatternConstants(Enum):
    BULLISH_GARTLEY: int = 1
    BEARISH_GARTLEY: int = -1
    BULLISH_CRAB: int = 2
    BEARISH_CRAB: int = -2
    BULLISH_BUTTERFLY: int = 3
    BEARISH_BUTTERFLY: int = -3
    BULLISH_BAT: int = 4
    BEARISH_BAT: int = -4
    BULLISH_ALT_BAT: int = 5
    BEARISH_ALT_BAT: int = -5
    BULLISH_SHARK: int = 6
    BEARISH_SHARK: int = -6
    BULLISH_CYPHER: int = 7
    BEARISH_CYPHER: int = -7
    BULLISH_DEEP_CRAB: int = 8
    BEARISH_DEEP_CRAB: int = -8
    BULLISH_121: int = 9
    BEARISH_121: int = -9
    BULLISH_LEONARDO: int = 9
    BEARISH_LEONARDO: int = -9
    BULLISH_WHITE_SWAN: int = 10
    BEARISH_WHITE_SWAN: int = -10
    BULLISH_BLACK_SWAN: int = 11
    BEARISH_BLACK_SWAN: int = -11
    BULLISH_NEN_STAR: int = 12
    BEARISH_NEN_STAR: int = -12
    BULLISH_3_DRIVES: int = 13
    BEARISH_3_DRIVES: int = -13

# http://chartreaderpro.com/harmonic-pattern-ratios/
harmonic_ratios = {
    PatternConstants.BULLISH_GARTLEY: {
        "AB-XA": 0.618,
        "BC-AB": [0.382, 0.886],
        "CD-BC": [1.27, 1.618],
        "D": 0.786
    },
    PatternConstants.BULLISH_CRAB: {
        "AB-XA": [0.382, 0.618],
        "BC-AB": [0.382, 0.886],
        "CD-BC": [2.24, 3.618],
        "D": 1.618
    },
    PatternConstants.BULLISH_BUTTERFLY: {
        "AB-XA": 0.786,
        "BC-AB": [0.382, 0.886],
        "CD-BC": [1.618, 2.618],
        "D": [1.27, 1.618]
    },
    PatternConstants.BULLISH_BAT: {
        "AB-XA": [0.382, 0.5],
        "BC-AB": [0.382, 0.886],
        "CD-BC": [1.618, 2.618],
        "D": 0.886
    },
    PatternConstants.BULLISH_ALT_BAT: {
        "AB-XA": 0.382,
        "BC-AB": [0.382, 0.886],
        "CD-BC": [2.0, 3.618],
        "D": 1.13
    },
    PatternConstants.BULLISH_SHARK: {
        "AB-XA": None,
        "BC-AB": [1.13, 1.618],
        "CD-BC": [1.618, 2.24],
        "D": [0.886, 1.13]
    },
    PatternConstants.BULLISH_CYPHER: {
        "AB-XA": [0.382, 0.618],
        "BC-AB": [1.272, 1.414],
        "CD-BC": None,
        "D": 0.786
    },
    PatternConstants.BULLISH_DEEP_CRAB: {
        "AB-XA": 0.886,
        "BC-AB": [0.382, 0.886],
        "CD-BC": [2.0, 3.618],
        "D": 1.618
    },
    PatternConstants.BULLISH_121: {
        "AB-XA": [0.5, 0.786],
        "BC-AB": [1.128, 3.618],
        "CD-BC": [0.382, 0.786],
        "D": [0.382, 0.786]
    },
    PatternConstants.BULLISH_LEONARDO: {
        "AB-XA": 0.5,
        "BC-AB": [0.382, 0.886],
        "CD-BC": [1.128, 2.618],
        "D": 0.786
    },
    PatternConstants.BULLISH_WHITE_SWAN: {
        "AB-XA": [0.382, 0.786],
        "BC-AB": [2.0, 4.237],
        "CD-BC": [0.5, 0.886],
        "D": [0.238, 0.886]
    },
    PatternConstants.BULLISH_BLACK_SWAN: {
        "AB-XA": [1.382, 2.618],
        "BC-AB": [0.236, 0.5],
        "CD-BC": [1.128, 2.0],
        "D": [1.128, 2.618]
    },
    PatternConstants.BULLISH_NEN_STAR: {
        "AB-XA": [0.382, 0.618],
        "BC-AB": [1.414, 2.14],
        "CD-BC": [1.272, 2.0],
        "D": 1.272
    },
    PatternConstants.BULLISH_3_DRIVES: {
        "AB-XA": [1.272, 1.618],
        "BC-AB": [0.618, 0.786],
        "CD-BC": [1.272, 1.618],
        "D": [1.618, 2.618]
    },
}


def __identify_harmonic_pattern(price_history: PriceHistory, error_rate: Optional[float] = 0.05) -> Union[int, PatternConstants]:
    df = price_history.price_history
    zz_up_indices = df[df[IndicatorRegistry.ZIGZAG.value] == 1.0].index
    zz_down_indices = df[df[IndicatorRegistry.ZIGZAG.value] == -1.0].index
    if len(zz_up_indices) + len(zz_down_indices) < 5:
        return 0

    pattern_side = ""
    if zz_up_indices[-1] > zz_down_indices[-1]:
        pattern_side = "SELL"
        zz_up_indices = zz_up_indices[-3:]
        zz_down_indices = zz_down_indices[-2:]
    else:
        pattern_side = "BUY"
        zz_up_indices = zz_up_indices[-2:]
        zz_down_indices = zz_down_indices[-3:]

    if pattern_side == "BUY":
        point_x = df[Column.LOW].loc[zz_down_indices[0]]
        point_a = df[Column.HIGH].loc[zz_up_indices[0]]
        point_b = df[Column.LOW].loc[zz_down_indices[1]]
        point_c = df[Column.HIGH].loc[zz_up_indices[1]]
        point_d = df[Column.LOW].loc[zz_down_indices[2]]
        xa = point_a - point_x
        ab = point_a - point_b
        bc = point_c - point_b
        cd = point_c - point_d
        ad = point_a - point_d
    else:
        point_x = df[Column.HIGH].loc[zz_up_indices[0]]
        point_a = df[Column.LOW].loc[zz_down_indices[0]]
        point_b = df[Column.HIGH].loc[zz_up_indices[1]]
        point_c = df[Column.LOW].loc[zz_down_indices[1]]
        point_d = df[Column.HIGH].loc[zz_up_indices[2]]

        xa = point_x - point_a
        ab = point_b - point_a
        bc = point_b - point_c
        cd = point_d - point_c
        ad = point_d - point_a

    # Weird, weird edge case
    if not xa or not ab or not bc or not cd or not ad:
        return 0

    pattern = abs(__check_pattern(xa, ab, bc, cd, ad, error=error_rate))
    if pattern_side == "BUY":
        return pattern
    else:
        return -pattern


def __check_pattern(xa: float, ab: float, bc: float, cd: float, ad: float, error: Optional[float] = 0.05) -> Union[int, PatternConstants]:
    """
    Compares legs against Harmonic ratios

    :param xa: Leg XA
    :param ab: Leg AB
    :param bc: Leg BC
    :param cd: Leg CD
    :param ad: Leg AD
    :param error: Error rate
    :return: PatternConstant if found, else 0
    :raises IndicatorException: If the pattern ratios are not defined
    """
    for pattern in PatternConstants:
        if pattern.value < 0:
            continue
        if pattern not in harmonic_ratios.keys():
            raise IndicatorException(f"Harmonic Pattern {pattern} ratios not defined")

        p_rat = harmonic_ratios[pattern]
        count = 0

        if isinstance(p_rat["AB-XA"], list):
            lower_xa = p_rat["AB-XA"][0]
            upper_xa = p_rat["AB-XA"][1]
        elif isinstance(p_rat["AB-XA"], type(None)):
            lower_xa = None
            upper_xa = None
        else:
            lower_xa = p_rat["AB-XA"] - (p_rat["AB-XA"] * error)
            upper_xa = p_rat["AB-XA"] + (p_rat["AB-XA"] * error)

        if isinstance(p_rat["D"], list):
            lower_d = p_rat["D"][0]
            upper_d = p_rat["D"][1]
        else:
            lower_d = p_rat["D"] - (p_rat["D"] * error)
            upper_d = p_rat["D"] + (p_rat["D"] * error)

        if isinstance(p_rat["AB-XA"], type(None)) or upper_xa >= ab / xa >= lower_xa:
            count += 1
        if p_rat["BC-AB"][1] >= bc / ab >= p_rat["BC-AB"][0]:
            count += 1
        if isinstance(p_rat["CD-BC"], type(None)) or p_rat["CD-BC"][1] >= cd / bc >= p_rat["CD-BC"][0]:
            count += 1
        if upper_d >= ad / xa >= lower_d:
            count += 1

        if count == 4:
            return pattern.value
    return 0


def get_harmonics_name(pattern: PatternConstants) -> str:
    """
    Get string name of Harmonics pattern

    :param pattern: One of PatternConstants
    :return: String representation of pattern
    """
    if pattern == PatternConstants.BULLISH_GARTLEY.value:
        return "BULLISH_GARTLEY"
    elif pattern == PatternConstants.BEARISH_GARTLEY.value:
        return "BEARISH_GARTLEY"
    elif pattern == PatternConstants.BULLISH_CRAB.value:
        return "BULLISH_CRAB"
    elif pattern == PatternConstants.BEARISH_CRAB.value:
        return "BEARISH_CRAB"
    elif pattern == PatternConstants.BULLISH_BUTTERFLY.value:
        return "BULLISH_BUTTERFLY"
    elif pattern == PatternConstants.BEARISH_BUTTERFLY.value:
        return "BEARISH_BUTTERFLY"
    elif pattern == PatternConstants.BULLISH_BAT.value:
        return "BULLISH_BAT"
    elif pattern == PatternConstants.BEARISH_BAT.value:
        return "BEARISH_BAT"
    elif pattern == PatternConstants.BULLISH_ALT_BAT.value:
        return "BULLISH_ALT_BAT"
    elif pattern == PatternConstants.BEARISH_ALT_BAT.value:
        return "BEARISH_ALT_BAT"
    elif pattern == PatternConstants.BULLISH_SHARK.value:
        return "BULLISH_SHARK"
    elif pattern == PatternConstants.BEARISH_SHARK.value:
        return "BEARISH_SHARK"
    elif pattern == PatternConstants.BULLISH_CYPHER.value:
        return "BULLISH_CYPHER"
    elif pattern == PatternConstants.BEARISH_CYPHER.value:
        return "BEARISH_CYPHER"
    elif pattern == PatternConstants.BULLISH_DEEP_CRAB.value:
        return "BULLISH_DEEP_CRAB"
    elif pattern == PatternConstants.BEARISH_DEEP_CRAB.value:
        return "BEARISH_DEEP_CRAB"
    elif pattern == PatternConstants.BULLISH_121.value:
        return "BULLISH_121"
    elif pattern == PatternConstants.BEARISH_121.value:
        return "BEARISH_121"
    elif pattern == PatternConstants.BULLISH_LEONARDO.value:
        return "BULLISH_LEONARDO"
    elif pattern == PatternConstants.BEARISH_LEONARDO.value:
        return "BEARISH_LEONARDO"
    elif pattern == PatternConstants.BULLISH_WHITE_SWAN.value:
        return "BULLISH_WHITE_SWAN"
    elif pattern == PatternConstants.BEARISH_WHITE_SWAN.value:
        return "BEARISH_WHITE_SWAN"
    elif pattern == PatternConstants.BULLISH_BLACK_SWAN.value:
        return "BULLISH_BLACK_SWAN"
    elif pattern == PatternConstants.BEARISH_BLACK_SWAN.value:
        return "BEARISH_BLACK_SWAN"
    elif pattern == PatternConstants.BULLISH_NEN_STAR.value:
        return "BULLISH_NEN_STAR"
    elif pattern == PatternConstants.BEARISH_NEN_STAR.value:
        return "BEARISH_NEN_STAR"
    elif pattern == PatternConstants.BULLISH_3_DRIVES.value:
        return "BULLISH_3_DRIVES"
    elif pattern == PatternConstants.BEARISH_3_DRIVES.value:
        return "BEARISH_3_DRIVES"
    elif pattern == 0:
        return "NO_PATTERN"
    else:
        raise IndicatorException(f"Unknown Harmonic pattern {pattern}")


def get_closest_harmonic(price_history: PriceHistory, index: Optional[Union[pd.Timestamp, int]] = -1, verbose: Optional[bool] = False) -> int:
    """
    Finds proximal harmonic pattern at the ZigZag point, or it's previous points, starting at index.

    :param price_history: The price history
    :param index: Optional index, integer or timestamp
    :param verbose: Verbostiy
    :return: Harmonic constant or 0 if none found
    """

    df = price_history.price_history

    index = standardize_index(price_history, index)

    end_index = -1
    start_index = -1
    direction = 0

    def find_start(end_index) -> int:
        si = None
        for j in range(end_index - 1, 0, -1):
            if df[IndicatorRegistry.ZIGZAG_REPAINT.value].iloc[j] == 0:
                si = j
                break
        return si

    for i in range(index, 0, -1):
        if df[IndicatorRegistry.ZIGZAG.value].iloc[i] == 1:
            end_index = i
            start_index = find_start(end_index)
            direction = 1
            break
        if df[IndicatorRegistry.ZIGZAG.value].iloc[i] == -1:
            end_index = i
            start_index = find_start(end_index)
            direction = -1
            break

    if direction == 0:
        return 0

    harmonic = 0
    if end_index and start_index:
        for x in range(end_index, start_index, -1):
            if df[IndicatorRegistry.HARMONIC.value].iloc[x] != 0:
                harmonic = df[IndicatorRegistry.HARMONIC.value].iloc[x]
                break

    if harmonic and verbose:
        print(f"Found harmonic: {get_harmonics_name(harmonic)} on {price_history.instrument.symbol} {price_history.timeframe}")
    return harmonic




