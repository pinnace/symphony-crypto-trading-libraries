from symphony.indicator_v2 import IndicatorRegistry
from symphony.data_classes import PriceHistory, copy_price_history
from symphony.exceptions import IndicatorException
from symphony.config import LOG_LEVEL, USE_MODIN
import numpy as np
from enum import Enum, auto
from symphony.enum import Column
from symphony.utils import get_log_header, glh
import logging

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

logger = logging.getLogger(__name__)


class WaveConstants(Enum):
    WAVE_0: int = 0
    WAVE_1: int = 1
    WAVE_2: int = 2
    WAVE_3: int = 3
    WAVE_4: int = 4
    WAVE_5: int = 5
    WAVE_A: int = 6
    WAVE_B: int = 7
    WAVE_C: int = 8
    WAVE_1C1: int = 9
    WAVE_1C2: int = 10


def td_upwave(price_history: PriceHistory,
              log_level: logging = LOG_LEVEL,
              price_history_copy: bool = False
              ) -> PriceHistory:
    """
    Calculates the TD Up D-Wave according to Perl. Note that while the Up- and Down-
    Waves will be largely independent, some overlap is likely to occur in the
    respective A, B, and C legs

    :param price_history: Standard price history
    :param log_level: Optional logging level
    :param price_history_copy: Optionally return deep copy of price history
    :return: Either the original or copied price history
    :raises IndicatorException: If not enough bars (21) to start dwave calculation
    """

    logger.setLevel(log_level)
    if price_history_copy:
        price_history = copy_price_history(price_history)
        df = price_history.price_history
    else:
        df = price_history.price_history

    dwave_up = pd.Series(np.zeros(len(df), dtype="int32"))

    if len(df) < 21:
        df[IndicatorRegistry.DWAVE_UP.value] = dwave_up.values
        logger.info(
            f"{glh(price_history)}[{IndicatorRegistry.DWAVE_UP.value.upper()}] Not enough bars to calculate DWave")
        return price_history

    # TODO: Make Hash Table using WaveConstants

    wave_1_start_index: int = -1
    wave_2_start_index: int = -1
    wave_3_start_index: int = -1
    wave_4_start_index: int = -1
    wave_5_start_index: int = -1
    wave_A_start_index: int = -1
    wave_B_start_index: int = -1
    wave_C_start_index: int = -1
    wave_1_end_index: int = -1
    wave_2_end_index: int = -1
    wave_3_end_index: int = -1
    wave_4_end_index: int = -1
    wave_5_end_index: int = -1
    wave_A_end_index: int = -1
    wave_B_end_index: int = -1
    wave_C_end_index: int = -1

    def zero_indices():
        wave_1_start_index: int = -1
        wave_2_start_index: int = -1
        wave_3_start_index: int = -1
        wave_4_start_index: int = -1
        wave_5_start_index: int = -1
        wave_A_start_index: int = -1
        wave_B_start_index: int = -1
        wave_C_start_index: int = -1
        wave_1_end_index: int = -1
        wave_2_end_index: int = -1
        wave_3_end_index: int = -1
        wave_4_end_index: int = -1
        wave_5_end_index: int = -1
        wave_A_end_index: int = -1
        wave_B_end_index: int = -1
        wave_C_end_index: int = -1

    current_wave = WaveConstants.WAVE_0
    for index in df.index[21:]:
        int_index: int = df.index.get_loc(index)
        close_to_test: float = df[Column.CLOSE].loc[index]

        if current_wave == WaveConstants.WAVE_0:
            zero_indices()
            # 21-bar low-close
            if close_to_test == min([*df[Column.CLOSE].iloc[int_index - 21:int_index].tolist(), close_to_test]):
                wave_1_start_index: int = int_index
                current_wave = WaveConstants.WAVE_1C1
        elif current_wave == WaveConstants.WAVE_1C1:
            dwave_up.iloc[int_index] = WaveConstants.WAVE_1.value
            # 13 bar high-close
            if close_to_test == max([*df[Column.CLOSE].iloc[int_index - 13:int_index].tolist(), close_to_test]):
                current_wave = WaveConstants.WAVE_1C2
        elif current_wave == WaveConstants.WAVE_1C2:
            dwave_up.iloc[int_index] = WaveConstants.WAVE_1.value
            # 8 bar low-close
            if close_to_test == min([*df[Column.CLOSE].iloc[int_index - 8:int_index].tolist(), close_to_test]):
                wave_1_end_index: int = int_index
                wave_2_start_index: int = int_index
                dwave_up.iloc[wave_1_start_index:wave_1_end_index] = WaveConstants.WAVE_1.value
                current_wave = WaveConstants.WAVE_2
            if close_to_test > max(df[Column.CLOSE].iloc[wave_1_start_index:int_index]):
                """
                if a pullback from TD D-Wave 1 is so shallow that the decline fails to satisfy
                the conditions necessary to initiate TD D-Wave 2, and the market subsequently
                recovers above what had been the TD D-Wave 1 high close, then TD D-Wave 1
                will shift over to the right in line with the new high close. 
                """
                # TODO: What to do?
                pass
        elif current_wave == WaveConstants.WAVE_2:
            dwave_up.iloc[int_index] = WaveConstants.WAVE_2.value
            if close_to_test < min(df[Column.CLOSE].iloc[wave_1_start_index:wave_1_end_index]):
                """
                if TD D-Wave 2 closes below the low close of TD D-Wave 1, then TD D-Wave 1
                will disappear, and the count must begin anew. (Similarly, if the low close of
                TD D-Wave 4 closes below the low close of TD D-Wave 2, then TD D-Wave 2 will
                shift to where TD D-Wave 4 would otherwise have been.) 
                """
                logger.debug(
                    f"{glh(price_history)}[{IndicatorRegistry.DWAVE_UP.value.upper()}][!] Wave 2 closed below Wave 1 low-close. Wiping Wave 1 and resetting")
                dwave_up.iloc[wave_1_start_index:int_index+1] = 0
                current_wave = WaveConstants.WAVE_0
            # 21 bar high-close
            if close_to_test == max([*df[Column.CLOSE].iloc[int_index - 21:int_index].tolist(), close_to_test]):
                wave_2_end_index: int = int_index
                wave_3_start_index: int = int_index
                dwave_up.iloc[wave_2_start_index:wave_2_end_index] = WaveConstants.WAVE_2.value
                current_wave = WaveConstants.WAVE_3


        elif current_wave == WaveConstants.WAVE_3:
            dwave_up.iloc[int_index] = WaveConstants.WAVE_3.value
            # 13 bar low-close
            if close_to_test == min([*df[Column.CLOSE].iloc[int_index - 13:int_index].tolist(), close_to_test]):
                wave_3_end_index: int = int_index
                if max(df[Column.CLOSE].iloc[wave_3_start_index:wave_3_end_index]) \
                        < max(df[Column.CLOSE].iloc[wave_1_start_index:wave_1_end_index]):
                    """The peak close of TD D-Wave 3 must be higher than the peak close of TD D-Wave 1"""
                    logger.debug(
                        f"{glh(price_history)}[{IndicatorRegistry.DWAVE_UP.value.upper()}][!] Wave 3 peak close failed to exceed Wave 1's. Resetting.")
                    current_wave = WaveConstants.WAVE_0
                    continue
                wave_4_start_index: int = int_index
                dwave_up.iloc[wave_3_start_index:wave_3_end_index] = WaveConstants.WAVE_3.value
                current_wave = WaveConstants.WAVE_4
            elif close_to_test > max(df[Column.CLOSE].iloc[wave_3_start_index:int_index]):
                """
                if a pullback from TD D-Wave 3 is so shallow that the decline fails to satisfy
                the conditions necessary to initiate TD D-Wave 4, and the market subsequently
                recovers above what had been the TD D-Wave 3 high close, then TD D-Wave 3
                will shift to the right in line with the new high close. 
                """
                # TODO: What to do?
                pass

        elif current_wave == WaveConstants.WAVE_4:
            dwave_up.iloc[int_index] = WaveConstants.WAVE_4.value
            if close_to_test < min(df[Column.CLOSE].iloc[wave_2_start_index:wave_2_end_index]):
                """
                If the low close of TD D-Wave 4 closes below the low close of TD D-Wave 2, then TD D-Wave 2 will
                shift to where TD D-Wave 4 would otherwise have been.
                """
                logger.debug(
                    f"{glh(price_history)}[{IndicatorRegistry.DWAVE_UP.value.upper()}][!] "
                    f"Wave 4 closed below Wave 2 low-close. Wiping Wave 3 and 4 and moving back into Wave 2")
                dwave_up.iloc[wave_2_end_index:int_index + 1] = WaveConstants.WAVE_2.value
                current_wave = WaveConstants.WAVE_2

            # 24 bar high-close
            if close_to_test == max([*df[Column.CLOSE].iloc[int_index - 24:int_index].tolist(), close_to_test]):
                wave_4_end_index: int = int_index
                wave_5_start_index: int = int_index
                dwave_up.iloc[wave_4_start_index:wave_4_end_index] = WaveConstants.WAVE_4.value
                current_wave = WaveConstants.WAVE_5

        elif current_wave == WaveConstants.WAVE_5:
            dwave_up.iloc[int_index] = WaveConstants.WAVE_5.value
            # 13 bar low-close
            if close_to_test == min([*df[Column.CLOSE].iloc[int_index - 13:int_index].tolist(), close_to_test]):
                wave_5_end_index: int = int_index
                if max(df[Column.CLOSE].iloc[wave_5_start_index:wave_5_end_index]) < max(
                        df[Column.CLOSE].iloc[wave_3_start_index:wave_3_end_index]):
                    """the peak close of TD D-Wave 5 must be above the peak close of TD D-Wave 3."""
                    logger.debug(
                        f"{glh(price_history)}[{IndicatorRegistry.DWAVE_UP.value.upper()}][!] Wave 5 peak close failed to exceed Wave 3's. Resetting.")
                    current_wave = WaveConstants.WAVE_0
                    continue
                wave_A_start_index: int = int_index
                dwave_up.iloc[wave_5_start_index:wave_5_end_index] = WaveConstants.WAVE_5.value
                current_wave = WaveConstants.WAVE_A
            if close_to_test > max(df[Column.CLOSE].iloc[wave_5_start_index:int_index]):
                """
                if a pullback from TD D-Wave 5 is so shallow that the decline fails to satisfy
                the conditions necessary to initiate TD D-Wave A, and the market subsequently 
                recovers above what had been the high close of TD D-Wave 5, then
                TD D-Wave 5 will shift over to the right in line with the new high close. 
                """
                # TODO: What to do?
                pass
        elif current_wave == WaveConstants.WAVE_A:
            dwave_up.iloc[int_index] = WaveConstants.WAVE_A.value
            # 8 bar high-close
            if close_to_test == max([*df[Column.CLOSE].iloc[int_index - 8:int_index].tolist(), close_to_test]):
                wave_A_end_index: int = int_index
                wave_B_start_index: int = int_index
                dwave_up.iloc[wave_A_start_index:wave_A_end_index] = WaveConstants.WAVE_A.value
                current_wave = WaveConstants.WAVE_B
        elif current_wave == WaveConstants.WAVE_B:
            dwave_up.iloc[int_index] = WaveConstants.WAVE_B.value
            # 21 bar low-close
            if close_to_test == min([*df[Column.CLOSE].iloc[int_index - 21:int_index].tolist(), close_to_test]):
                wave_B_end_index: int = int_index
                wave_C_start_index: int = int_index
                dwave_up.iloc[wave_B_start_index:wave_B_end_index] = WaveConstants.WAVE_B.value
                current_wave = WaveConstants.WAVE_C
            if close_to_test > max(df[Column.CLOSE].iloc[wave_5_start_index:wave_5_end_index]):
                """
                TD D-Wave 5 will be locked into place only when TD D-Wave C violates the low
                close of TD D-Wave A on a closing basis. until that happens, if what had been
                TD D-Wave B closes above the high close of TD D-Wave 5, then TD D-Waves A
                and B will be erased, and TD D-Wave 5 will shift to the right. 
                """
                logger.debug(f"{glh(price_history)}[{IndicatorRegistry.DWAVE_UP.value.upper()}][!] Wave B violated "
                            f"Wave 5 High close. Moving back into Wave 5")
                dwave_up.iloc[wave_5_end_index:int_index+1] = WaveConstants.WAVE_5.value
                current_wave = WaveConstants.WAVE_5

        elif current_wave == WaveConstants.WAVE_C:
            dwave_up.iloc[int_index] = WaveConstants.WAVE_C.value
            if close_to_test < min(df[Column.CLOSE].iloc[wave_A_start_index:wave_A_end_index]):
                wave_C_end_index: int = int_index
                dwave_up.iloc[wave_C_start_index:wave_C_end_index] = WaveConstants.WAVE_C.value
                current_wave = WaveConstants.WAVE_0
            if close_to_test > max(df[Column.CLOSE].iloc[wave_5_start_index:wave_5_end_index]):
                """
                if the market subsequently closes back above
                the high close of TD D-Wave 5, rather than erasing TD D-Waves A, B, and C, and
                moving TD D-Wave 5 to the right,the indicator will instead label the move to new highs
                as a fresh TD D-Wave 1 advance rather than erasing the previous TD D-Wave 5.
                """
                logger.debug(f"{glh(price_history)}[{IndicatorRegistry.DWAVE_UP.value.upper()}][!] Wave C closed above "
                            f"Wave 5 High close. Writing out Wave C and moving back into Wave 1")
                wave_C_end_index: int = int_index
                dwave_up.iloc[wave_C_start_index:wave_C_end_index] = WaveConstants.WAVE_C.value
                zero_indices()
                wave_1_start_index: int = int_index
                current_wave = WaveConstants.WAVE_1C1
        else:
            raise IndicatorException(
                f"{glh(price_history)}[{IndicatorRegistry.DWAVE_UP.value.upper()}][!] Unknown Wave: {current_wave}")

    df[IndicatorRegistry.DWAVE_UP.value] = dwave_up.values
    return price_history


def td_downwave(price_history: PriceHistory,
                log_level: logging = LOG_LEVEL,
                price_history_copy: bool = False,
                ) -> PriceHistory:
    """
    Calculates the TD Down D-Wave according to Perl. Note that while the Up- and Down-
    Waves will be largely independent, some overlap is likely to occur in the
    respective A, B, and C legs

    :param price_history: Standard price history
    :param log_level: Optional logging level
    :param price_history_copy: Optionally return deep copy of price history
    :return: Either the original or copied price history
    :raises IndicatorException: If not enough bars (21) to start dwave calculation
    """
    logger.setLevel(log_level)
    if price_history_copy:
        price_history = copy_price_history(price_history)
        df = price_history.price_history
    else:
        df = price_history.price_history

    dwave_down = pd.Series(np.zeros(len(df), dtype="int32"))
    if len(df) < 21:
        df[IndicatorRegistry.DWAVE_DOWN.value] = dwave_down.values
        logger.info(
            f"{glh(price_history)}[{IndicatorRegistry.DWAVE_UP.value.upper()}] Not enough bars to calculate DWave")
        return price_history

    # TODO: Make Hash Table using WaveConstants

    wave_1_start_index: int = -1
    wave_2_start_index: int = -1
    wave_3_start_index: int = -1
    wave_4_start_index: int = -1
    wave_5_start_index: int = -1
    wave_A_start_index: int = -1
    wave_B_start_index: int = -1
    wave_C_start_index: int = -1
    wave_1_end_index: int = -1
    wave_2_end_index: int = -1
    wave_3_end_index: int = -1
    wave_4_end_index: int = -1
    wave_5_end_index: int = -1
    wave_A_end_index: int = -1
    wave_B_end_index: int = -1
    wave_C_end_index: int = -1

    def zero_indices():
        wave_1_start_index: int = -1
        wave_2_start_index: int = -1
        wave_3_start_index: int = -1
        wave_4_start_index: int = -1
        wave_5_start_index: int = -1
        wave_A_start_index: int = -1
        wave_B_start_index: int = -1
        wave_C_start_index: int = -1
        wave_1_end_index: int = -1
        wave_2_end_index: int = -1
        wave_3_end_index: int = -1
        wave_4_end_index: int = -1
        wave_5_end_index: int = -1
        wave_A_end_index: int = -1
        wave_B_end_index: int = -1
        wave_C_end_index: int = -1

    current_wave = WaveConstants.WAVE_0
    for index in df.index[21:]:
        int_index: int = df.index.get_loc(index)
        close_to_test: float = df[Column.CLOSE].loc[index]

        if current_wave == WaveConstants.WAVE_0:
            zero_indices()
            # 21-bar high-close
            if close_to_test == max([*df[Column.CLOSE].iloc[int_index - 21:int_index].tolist(), close_to_test]):
                wave_1_start_index: int = int_index
                current_wave = WaveConstants.WAVE_1C1
        elif current_wave == WaveConstants.WAVE_1C1:
            dwave_down.iloc[int_index] = WaveConstants.WAVE_1.value
            # 13 bar low-close
            if close_to_test == min([*df[Column.CLOSE].iloc[int_index - 13:int_index].tolist(), close_to_test]):
                current_wave = WaveConstants.WAVE_1C2
        elif current_wave == WaveConstants.WAVE_1C2:
            dwave_down.iloc[int_index] = WaveConstants.WAVE_1.value
            # 8 bar high-close
            if close_to_test == max([*df[Column.CLOSE].iloc[int_index - 8:int_index].tolist(), close_to_test]):
                wave_1_end_index: int = int_index
                wave_2_start_index: int = int_index
                dwave_down.iloc[wave_1_start_index:wave_1_end_index] = WaveConstants.WAVE_1.value
                current_wave = WaveConstants.WAVE_2
            if close_to_test < min(df[Column.CLOSE].iloc[wave_1_start_index:int_index]):
                """
                if a rebound from TD D-Wave 1 is so shallow that the advance fails to satisfy the
                conditions necessary to initiate TD D-Wave 2, and the market falls back below
                what had been the low close of TD D-Wave 1, then TD D-Wave 1 will shift to the
                right, in line with the new low close. 
                """
                # TODO: What to do?
                pass
        elif current_wave == WaveConstants.WAVE_2:
            dwave_down.iloc[int_index] = WaveConstants.WAVE_2.value
            if close_to_test > max(df[Column.CLOSE].iloc[wave_1_start_index:wave_1_end_index]):
                """
                if TD D-Wave 2 closes above the high close of TD D-Wave 1, then TD D-Wave 1
                will disappear. (Similarly, if TD D-Wave 4 closes above the high close of TD
                D-Wave 2, then TD D-Wave 2 will shift to the right to where TD D-Wave 4 would
                otherwise have been.)
                """
                logger.debug(
                    f"{glh(price_history)}[{IndicatorRegistry.DWAVE_DOWN.value.upper()}][!] Wave 2 closed above Wave 1 high-close. Wiping Wave 1 and resetting")
                dwave_down.iloc[wave_1_start_index:int_index+1] = 0
                current_wave = WaveConstants.WAVE_0
            # 21 bar low-close
            if close_to_test == min([*df[Column.CLOSE].iloc[int_index - 21:int_index].tolist(), close_to_test]):
                wave_2_end_index: int = int_index
                wave_3_start_index: int = int_index
                dwave_down.iloc[wave_2_start_index:wave_2_end_index] = WaveConstants.WAVE_2.value
                current_wave = WaveConstants.WAVE_3


        elif current_wave == WaveConstants.WAVE_3:
            dwave_down.iloc[int_index] = WaveConstants.WAVE_3.value
            # 13 bar high-close
            if close_to_test == max([*df[Column.CLOSE].iloc[int_index - 13:int_index].tolist(), close_to_test]):
                wave_3_end_index: int = int_index
                if min(df[Column.CLOSE].iloc[wave_3_start_index:wave_3_end_index]) \
                        > min(df[Column.CLOSE].iloc[wave_1_start_index:wave_1_end_index]):
                    """The trough close of TD D-Wave 3 must be lower than the trough close of TD D-Wave 1"""
                    logger.debug(
                        f"{glh(price_history)}[{IndicatorRegistry.DWAVE_DOWN.value.upper()}][!] Wave 3 trough close failed to exceed Wave 1's. Resetting.")
                    current_wave = WaveConstants.WAVE_0
                    continue
                wave_4_start_index: int = int_index
                dwave_down.iloc[wave_3_start_index:wave_3_end_index] = WaveConstants.WAVE_3.value
                current_wave = WaveConstants.WAVE_4
            elif close_to_test < min(df[Column.CLOSE].iloc[wave_3_start_index:int_index]):
                """
                if a rally from TD D-Wave 3 is so shallow that the advance fails to satisfy the
                conditions necessary to initiate TD D-Wave 4, and the market falls back below
                what had been the low close of TD D-Wave 3, then TD D-Wave 3 will shift to the
                right, in line with the new low close. 
                """
                # TODO: What to do?
                pass

        elif current_wave == WaveConstants.WAVE_4:
            dwave_down.iloc[int_index] = WaveConstants.WAVE_4.value
            if close_to_test > max(df[Column.CLOSE].iloc[wave_2_start_index:wave_2_end_index]):
                """
                if TD D-Wave 4 closes above the high close of TD
                D-Wave 2, then TD D-Wave 2 will shift to the right to where TD D-Wave 4 would
                otherwise have been
                """
                logger.debug(
                    f"{glh(price_history)}[{IndicatorRegistry.DWAVE_DOWN.value.upper()}][!] "
                    f"Wave 4 closed above Wave 2 high-close. Wiping Wave 3 and 4 and moving back into Wave 2")
                dwave_down.iloc[wave_2_end_index:int_index + 1] = WaveConstants.WAVE_2.value
                current_wave = WaveConstants.WAVE_2

            # 24 bar low-close
            if close_to_test == min([*df[Column.CLOSE].iloc[int_index - 24:int_index].tolist(), close_to_test]):
                wave_4_end_index: int = int_index
                wave_5_start_index: int = int_index
                dwave_down.iloc[wave_4_start_index:wave_4_end_index] = WaveConstants.WAVE_4.value
                current_wave = WaveConstants.WAVE_5


        elif current_wave == WaveConstants.WAVE_5:
            dwave_down.iloc[int_index] = WaveConstants.WAVE_5.value
            # 13 bar high-close
            if close_to_test == max([*df[Column.CLOSE].iloc[int_index - 13:int_index].tolist(), close_to_test]):
                wave_5_end_index: int = int_index
                if min(df[Column.CLOSE].iloc[wave_5_start_index:wave_5_end_index]) > min(
                        df[Column.CLOSE].iloc[wave_3_start_index:wave_3_end_index]):
                    """the trough close of TD D-Wave 5 must be below the trough close of TD D-Wave 3."""
                    logger.debug(
                        f"{glh(price_history)}[{IndicatorRegistry.DWAVE_DOWN.value.upper()}][!] Wave 5 trough close failed to exceed Wave 3's. Resetting.")
                    current_wave = WaveConstants.WAVE_0
                    continue
                wave_A_start_index: int = int_index
                dwave_down.iloc[wave_5_start_index:wave_5_end_index] = WaveConstants.WAVE_5.value
                current_wave = WaveConstants.WAVE_A
            if close_to_test < min(df[Column.CLOSE].iloc[wave_5_start_index:int_index]):
                """
                if a rally from TD D-Wave 5 is so shallow that the rebound fails to satisfy the
                conditions necessary to initiate TD D-Wave A, and the market sells off below what
                had been the low close of TD D-Wave 5, then TD D-Wave 5 will shift to the right,
                in line with the new low close
                """
                # TODO: What to do?
                pass
        elif current_wave == WaveConstants.WAVE_A:
            dwave_down.iloc[int_index] = WaveConstants.WAVE_A.value
            # 8 bar low-close
            if close_to_test == min([*df[Column.CLOSE].iloc[int_index - 8:int_index].tolist(), close_to_test]):
                wave_A_end_index: int = int_index
                wave_B_start_index: int = int_index
                dwave_down.iloc[wave_A_start_index:wave_A_end_index] = WaveConstants.WAVE_A.value
                current_wave = WaveConstants.WAVE_B
        elif current_wave == WaveConstants.WAVE_B:
            dwave_down.iloc[int_index] = WaveConstants.WAVE_B.value
            # 21 bar high-close
            if close_to_test == max([*df[Column.CLOSE].iloc[int_index - 21:int_index].tolist(), close_to_test]):
                wave_B_end_index: int = int_index
                wave_C_start_index: int = int_index
                dwave_down.iloc[wave_B_start_index:wave_B_end_index] = WaveConstants.WAVE_B.value
                current_wave = WaveConstants.WAVE_C
            if close_to_test < min(df[Column.CLOSE].iloc[wave_5_start_index:wave_5_end_index]):
                """
                TD D-Wave 5 will be locked into place only when TD D-Wave C violates the high
                close of TD D-Wave A. until that happens, if what had been TD D-Wave B trades
                below the low close of TD D-Wave 5, then TD D-Waves A and B will be erased,
                and TD D-Wave 5 will shift to the right. 
                """
                logger.debug(f"{glh(price_history)}[{IndicatorRegistry.DWAVE_DOWN.value.upper()}][!] Wave B violated "
                            f"Wave 5 low close. Moving back into Wave 5")
                dwave_down.iloc[wave_5_end_index:int_index + 1] = WaveConstants.WAVE_5.value
                current_wave = WaveConstants.WAVE_5

        elif current_wave == WaveConstants.WAVE_C:
            dwave_down.iloc[int_index] = WaveConstants.WAVE_C.value
            if close_to_test > max(df[Column.CLOSE].iloc[wave_A_start_index:wave_A_end_index]):
                wave_C_end_index: int = int_index
                dwave_down.iloc[wave_C_start_index:wave_C_end_index] = WaveConstants.WAVE_C.value
                current_wave = WaveConstants.WAVE_0
            if close_to_test < min(df[Column.CLOSE].iloc[wave_5_start_index:wave_5_end_index]):
                """
                if the market subsequently closes back below the low close of TD D-Wave 5, 
                rather than erasing TD DWaves A, B, and C, and moving TD D-Wave 5 to the right, the indicator will instead
                label the move to new highs as a fresh TD D-Wave 1 advance rather than erasing the previous TD D-Wave 5.
                """
                logger.debug(
                    f"{glh(price_history)}[{IndicatorRegistry.DWAVE_DOWN.value.upper()}][!] Wave C closed below "
                    f"Wave 5 low close. Writing out Wave C and moving back into Wave 1")
                wave_C_end_index: int = int_index
                dwave_down.iloc[wave_C_start_index:wave_C_end_index] = WaveConstants.WAVE_C.value
                zero_indices()
                wave_1_start_index: int = int_index
                current_wave = WaveConstants.WAVE_1C1
        else:
            raise IndicatorException(
                f"{glh(price_history)}[{IndicatorRegistry.DWAVE_DOWN.value.upper()}][!] Unknown Wave: {current_wave}")

    df[IndicatorRegistry.DWAVE_DOWN.value] = dwave_down.values
    return price_history
