import numpy as np
from typing import List
from symphony.indicators.indicator_kit import IndicatorKit
from symphony.schema.schema_utils import schema

@schema(filename='indicators/demarkcountdown/bullish_price_flip_schema.json')
def bullish_price_flip(flow: dict, window_size: int = 6) -> dict:
    """
    Calculates bullish price flips over a ohlc_history object (array of OHLC bars)

    Args:
        flow (np.array | list): Indicator flow
        window_size (int): The frame at which the indicator is looking over
    
    Returns:
        list:Array 1s and 0s, where 1s are locations of completed price flips

    """
    
    close_prices = IndicatorKit.get_points(flow, "close")

    price_flips = np.zeros(len(close_prices), dtype=int)
    for index in range(window_size - 1,len(close_prices)):
        assert(index - 5 >= 0)
        if close_prices[index] > close_prices[index-4] and close_prices[index-1] < close_prices[index-5]:
            price_flips[index] = 1

    return {
        "bullish_price_flips" : price_flips.tolist()
    }
    
@schema(filename='indicators/demarkcountdown/bearish_price_flip_schema.json')
def bearish_price_flip(flow: dict, window_size: int = 6) -> dict:
    """
    Calculates bearish price flips over a ohlc_history object (array of OHLC bars)

    Args:
        ohlc_history (np.array | list): Array of OHLC bars
        window_size (int): The frame at which the indicator is looking over
    
    Returns:
        list:Array 1s and 0s, where 1s are locations of completed price flips

    """
    close_prices = IndicatorKit.get_points(flow, "close")

    price_flips = np.zeros(len(close_prices), dtype=int)
    for index in range(window_size - 1,len(close_prices)):
        assert(index - 5 >= 0)
        if close_prices[index] < close_prices[index-4] and close_prices[index-1] > close_prices[index-5]:
            price_flips[index] = 1

    return {
        "bearish_price_flips" : price_flips.tolist()
    }




