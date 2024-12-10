
import numpy as np
from symphony.indicators import IndicatorKit, IndicatorRegistry, IndicatorError


def get_td_countdown_pattern_start(flow: dict, countdown_index: int) -> int:
    """
    Fetches the starting index of the whole pattern sequence. E.g. if an
    `index` belonging to a BUY countdown is supplied, will return the index 
    of the price flip kicking off the corresponding BUY setup

    Args:
        flow (dict): A flow object
        countdown_index (int): Index of a BUY or SELL countdown

    Return:
        (int): Index of the first bar of the price flip of the corresponding
                setup
    """
    countdowns = IndicatorKit.get_indicator(flow, IndicatorRegistry.TD_COUNTDOWN)
    if countdowns["buy_countdowns"][countdown_index]:
        buy_setup_indices = np.where(np.array(countdowns["active_buy_setups"]) == 1)[0]
        setup_index = [i for i in buy_setup_indices if i < countdown_index][-1]
    
    elif countdowns["sell_countdowns"][countdown_index]:
        sell_setup_indices = np.where(np.array(countdowns["active_sell_setups"]) == 1)[0]
        setup_index = [i for i in sell_setup_indices if i < countdown_index][-1]
    else:
        raise IndicatorError(__name__ + ": No matching countdown for {} index found".format(countdown_index))
    
    return setup_index - 14

def get_td_countdown_stoploss(flow: dict, countdown_index: int) -> float:
    """
    Get the stoploss for a pattern.

    Args:
        flow (dict): A flow object
        countdown_index (int): Index of a BUY or SELL countdown

    Return:
        (float):Stoploss
    """
    start_index = get_td_countdown_pattern_start(flow, countdown_index)
    countdowns = IndicatorKit.get_indicator(flow, IndicatorRegistry.TD_COUNTDOWN)
    if countdowns["buy_countdowns"][countdown_index]:
        
        lows = IndicatorKit.get_points(flow, "low")
        min_index, min_value = min(enumerate(lows[start_index:countdown_index+1]), key=lambda p: p[1])
        min_index = min_index + start_index
        true_low = IndicatorKit.get_true_low(flow, min_index)
        true_range = IndicatorKit.get_true_range_candle(flow, min_index)
        stop_loss = round(true_low - true_range, flow["digits"])
        
    elif countdowns["sell_countdowns"][countdown_index]:
        highs = IndicatorKit.get_points(flow, "low")
        max_index, max_value = max(enumerate(highs[start_index:countdown_index+1]), key=lambda p: p[1])
        max_index = max_index + start_index
        true_high = IndicatorKit.get_true_high(flow, max_index)
        true_range = IndicatorKit.get_true_range_candle(flow, max_index)
        stop_loss = round(true_high + true_range, flow["digits"])
    else:
        raise IndicatorError(__name__ + ": No matching countdown for {} index found".format(countdown_index))

    return stop_loss

def get_td_pattern_ohclv(flow: dict, pattern_start: int) -> dict:
    """
    Return five arrays (OHLCV) wrapped in a dict corresponding to the pattern start

    Args:
        flow (dict): A standard flow
        pattern_start (int): Pattern start index

    Returns
        (dict): {"opens" : [...], "highs" : [...], "lows": [...], "closes": [...], "volumes": [...]}
    """

    pattern_values = {
        "opens": [],
        "highs": [],
        "lows" : [],
        "closes" : [],
        "volumes" : []
    }
    ohlcv_values = flow["price_history"][pattern_start:]
    for bar in ohlcv_values:
        pattern_values["opens"].append(bar[0])
        pattern_values["highs"].append(bar[1])
        pattern_values["lows"].append(bar[2])
        pattern_values["closes"].append(bar[3])
        pattern_values["volumes"].append(bar[4])

    return pattern_values