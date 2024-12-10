
from typing import List
import numpy as np
from symphony.indicators.indicator_kit import IndicatorKit
from symphony.indicators.indicator_registry import IndicatorRegistry
from symphony.indicators.indicator_exception import IndicatorError
from symphony.schema.schema_utils import schema

buy_setup_keys =  ["buy_setups", "buy_setups_true_end", "perfect_buy_setups", "tdst_resistance", "tradeable_buy_setups" ]
sell_setup_keys = ["sell_setups", "sell_setups_true_end", "perfect_sell_setups", "tdst_support", "tradeable_sell_setups"]

@schema(filename='indicators/demarkcountdown/td_buy_setup_schema.json')
def td_buy_setup(flow: dict, window_size: int = 4, bearish_price_flips: list = []) -> dict:
    """
    Calculate the buy setup. Can take either price flip from parameter or fetch from flow.
        Parameter takes precedence.

    Args:
        flow (dict): Flow object
        window_size (int, optional): Lookback
        bearish_price_flips (list, optional): Optional price flips to use
    
    Returns:
        dict: Channel name "buy_setups", "perfect_buy_setups", "tdst_resistance" (ragged)
    """
    
    if bearish_price_flips:
        if not isinstance(bearish_price_flips, list):
            raise IndicatorError(__name__ + ": Price flips not an array")
        price_flips = bearish_price_flips

    else:
        price_flips = IndicatorKit.get_indicator(flow, IndicatorRegistry.BEARISH_PRICE_FLIP, channel_names="bearish_price_flips")[0]

    price_flips = np.array(price_flips)

    flip_indices = np.where(price_flips == 1.0)[0]

    close_prices = IndicatorKit.get_points(flow, "close")

    setups = np.zeros(len(close_prices), dtype=int)
    perfect_setups = np.zeros(len(close_prices), dtype=int)
    tdst_resistance = np.zeros(len(close_prices), dtype=float)

    for i in flip_indices:
        if len(close_prices[i:]) >= 9:
            for x in range(i, i+9):
                if close_prices[x] < close_prices[x-4]:
                    if x == i + 8:
                        setups[x] = 1
                        

                        # Get TDST Resistance
                        tdst_resistance[x:] = IndicatorKit.get_true_high(flow, i)

                        # Check if perfect
                        # The low of bars eight or nine of the TD Buy Setup or a subsequent low must be less
                        # than, or equal to, the lows of bars six and seven of the TD Buy Setup.
                        low_prices = IndicatorKit.get_points(flow, "low")
                        if (low_prices[x] <= low_prices[x-2] and low_prices[x] <= low_prices[x-3]) or \
                            (low_prices[x-1] <= low_prices[x-2] and low_prices[x - 1] <= low_prices[x-3]):
                            perfect_setups[x] = 1


                else:
                    break

    # Get the true setup range
    # Setup range can extend beyond bar 9
    setup_true_end = np.zeros(len(close_prices), dtype=int)
    for setup_index in np.where(setups == 1)[0]:
        for i in range(setup_index + 1, len(close_prices)):
            if close_prices[i] < close_prices[i-4]:
                continue
            else:
                # Instead of a 1, hold the setup index (e.g. 42) at the spot of the true end
                setup_true_end[setup_index] = i
                break
        if not setup_true_end[setup_index]:
            setup_true_end[setup_index] = setup_index

    tdst_resistance = IndicatorKit.strip_nulls(tdst_resistance)
    return {
        "buy_setups" : setups.tolist(),
        "buy_setups_true_end" : setup_true_end.tolist(),
        "perfect_buy_setups" : perfect_setups.tolist(),
        "tdst_resistance" : tdst_resistance.tolist()
        }

@schema(filename='indicators/demarkcountdown/td_sell_setup_schema.json')
def td_sell_setup(flow: dict, window_size: int = 4, bullish_price_flips: list = []) -> dict:
    """
    Calculate the sell setup. Can take either price flip from parameter or fetch from flow.
        Parameter takes precedence.

    Args:
        flow (dict): Flow object
        window_size (int, optional): Lookback
        bullish_price_flips (list, optional): Optional price flips to use
    
    Returns:
        dict: Channel name "sell_setups", "perfect_sell_setups", "tdst_support" (ragged)
    """
    
    if bullish_price_flips:
        if not isinstance(bullish_price_flips, list):
            raise IndicatorError(__name__ + ": Price flips not an array")
        price_flips = bullish_price_flips
    else:
        price_flips = IndicatorKit.get_indicator(flow, IndicatorRegistry.BULLISH_PRICE_FLIP, channel_names="bullish_price_flips")[0]

    price_flips = np.array(price_flips)

    flip_indices = np.where(price_flips == 1.0)[0]

    close_prices = IndicatorKit.get_points(flow, "close")

    setups = np.zeros(len(close_prices), dtype=int)
    perfect_setups = np.zeros(len(close_prices), dtype=int)
    tradeable_setups = np.zeros(len(close_prices), dtype=int)
    tdst_support = np.zeros(len(close_prices), dtype=float)

    for i in flip_indices:
        if len(close_prices[i:]) >= 9:
            for x in range(i, i+9):
                if close_prices[x] > close_prices[x-4]:
                    if x == i + 8:
                        setups[x] = 1


                        # Get TDST Support
                        tdst_support[x:] = IndicatorKit.get_true_low(flow, i)

                        # Check if perfect
                        # The high of bars eight or nine of the TD Buy Setup or a subsequent high must be greater
                        # than, or equal to, the highs of bars six and seven of the TD Buy Setup.
                        high_prices = IndicatorKit.get_points(flow, "high")
                        if (high_prices[x] >= high_prices[x-2] and high_prices[x] >= high_prices[x-3]) or \
                            (high_prices[x-1] >= high_prices[x-1] and high_prices[x] >= high_prices[x-2]):
                            perfect_setups[x] = 1
                else:
                    break
    
    # Get the true setup range
    # Setup range can extend beyond bar 9
    setup_true_end = np.zeros(len(close_prices), dtype=int)

    for setup_index in np.where(setups == 1)[0]:
        for i in range(setup_index + 1, len(close_prices)):
            if close_prices[i] > close_prices[i-4]:
                continue
            else:
                # Instead of a 1, hold the setup index (e.g. 42) at the spot of the true end
                setup_true_end[setup_index] = i
                break
        if not setup_true_end[setup_index]:
            setup_true_end[setup_index] = setup_index
    
    tdst_support = IndicatorKit.strip_nulls(tdst_support)

    return {
        "sell_setups" : setups.tolist(),
        "sell_setups_true_end" : setup_true_end.tolist(),
        "perfect_sell_setups" : perfect_setups.tolist(),
        "tdst_support" : tdst_support.tolist()
    }

@schema(filename='indicators/demarkcountdown/td_setup_schema.json')
def td_setup(flow: dict, bullish_price_flips: List[int] = [], bearish_price_flips: List[int] = [], tradeable_setup_true_range_alpha: float = 1.0, **kwargs) -> dict:
    """
    Will merge the buy and sell setup functions and identify tradeable setups based on Perl's rules

    Args:
        flow (dict): Flow object,
        bullish_price_flips (list, optional): Bullish price flips
        bearish_price_flips (list, optional): Bearish price flips
        tradeable_setup_true_range_alpha (float, optional): The buy setup's true low should be > (tdst_support + true_range(bar_9) * tradeable_setup_true_range_alpha)
                                            in order to be tradable. Defaults to 0.

    Returns:
        (dict): Unified TD Setups object
    """

    buy_setups = td_buy_setup(flow, bearish_price_flips=bearish_price_flips)
    sell_setups = td_sell_setup(flow, bullish_price_flips=bullish_price_flips)
    close_prices = IndicatorKit.get_points(flow, "close")

    # Identify tradeable sell setups

    # Pad TDST with 0s
    tdst_resistance = IndicatorKit.pad_to_length(buy_setups["tdst_resistance"], close_prices.size)
    tdst_support = IndicatorKit.pad_to_length(sell_setups["tdst_support"], close_prices.size)
    """
    tdst_resistance = ([0.0] * (close_prices.size - len(buy_setups["tdst_resistance"]))) + buy_setups["tdst_resistance"] \
                        if len(buy_setups["tdst_resistance"]) < close_prices.size else buy_setups["tdst_resistance"]
    tdst_support = ([0.0] * (close_prices.size - len(sell_setups["tdst_support"]))) + sell_setups["tdst_support"] \
                        if len(sell_setups["tdst_support"]) < close_prices.size else sell_setups["tdst_support"]
    """
    
    tradeable_buy_setups = np.zeros(len(close_prices), dtype=int)
    tradeable_sell_setups = np.zeros(len(close_prices), dtype=int)
    perfect_buy_setups = np.array(buy_setups["perfect_buy_setups"])
    perfect_sell_setups = np.array(sell_setups["perfect_sell_setups"])

    # For all perfect buy setups
    # Rules:
    # 1. When the TD Buy Setup has been perfected, that is, the low of TD Buy Setup bar
    #       eight or nine is less than the lows of TD Buy Setup bars six and seven,
    # 2. When none of the bars within the TD Buy Setup has closed below TDST support, and
    # 3. When the close of TD Buy Setup bar nine is in close proximity to TDST support. 

    for perfect_buy_setup_index in np.where(perfect_buy_setups == 1)[0]:
        tdst_support_value = tdst_support[perfect_buy_setup_index]
        if tdst_support_value:
            if min(close_prices[perfect_buy_setup_index - 8: perfect_buy_setup_index + 1]) >= tdst_support_value:

                # Proximity heuristic
                true_range_bar_9 = IndicatorKit.get_true_range_candle(flow, perfect_buy_setup_index)
                if (close_prices[perfect_buy_setup_index] - tdst_support_value) < (true_range_bar_9 * tradeable_setup_true_range_alpha):
                    tradeable_buy_setups[perfect_buy_setup_index] = 1
    # For all perfect sell setups
    # Rules:
    # 1. When the TD Sell Setup has been perfected, that is, when the high of TD Sell Setup
    #       bar eight or nine is greater than the highs of TD Sell Setup bars six and seven,
    # 2. When none of the bars within the TD Sell Setup has closed above TDST resistance, and
    # 3. When the close of TD Buy Setup bar nine is in close proximity to TDST resistance. 

    for perfect_sell_setup_index in np.where(perfect_sell_setups == 1)[0]:
        tdst_resistance_value = tdst_resistance[perfect_sell_setup_index]
        if tdst_resistance_value:

            # All closes must be below resistance
            if max(close_prices[perfect_sell_setup_index - 8: perfect_sell_setup_index + 1]) <= tdst_resistance_value:
                # Proximity heuristic
                true_range_bar_9 = IndicatorKit.get_true_range_candle(flow, perfect_sell_setup_index)
                if (close_prices[perfect_sell_setup_index] - tdst_resistance_value) < (true_range_bar_9 * tradeable_setup_true_range_alpha):
                    tradeable_sell_setups[perfect_sell_setup_index] = 1

    return {
        **buy_setups,
        **sell_setups,
        "tradeable_buy_setups" : tradeable_buy_setups.tolist(),
        "tradeable_sell_setups" : tradeable_sell_setups.tolist()
    }