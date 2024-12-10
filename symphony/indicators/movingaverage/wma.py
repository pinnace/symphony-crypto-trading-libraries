import numpy as np
from symphony.indicators.indicator_kit import IndicatorKit
from symphony.schema.schema_kit import schema
import talib
from numpy import isnan

@schema(filename="indicators/movingaverage/wma_schema.json")
def wma(flow: dict, period: int = 12) -> dict:
    """
    Weighted moving average

    Args:
        flow (dict): Flow object
        period (int, optional): The WMA period. Defaults to 12

    Returns:
        (dict): WMA
    """
    wma = talib.WMA(IndicatorKit.get_points(flow, "close"), timeperiod=period)
    wma = wma[~isnan(wma)].tolist()
    return {
        "wma" : wma
    }