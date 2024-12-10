import numpy as np
from symphony.indicators.indicator_kit import IndicatorKit
from symphony.schema.schema_kit import schema
import talib
from numpy import isnan

@schema(filename="indicators/movingaverage/ema_schema.json")
def ema(flow: dict, period: int = 12) -> dict:
    """
    Exponential moving average

    Args:
        flow (dict): Flow object
        period (int, optional): The EMA period. Defaults to 12

    Returns:
        (dict): EMA
    """
    ema = talib.EMA(IndicatorKit.get_points(flow, "close"), timeperiod=period)
    ema = ema[~isnan(ema)].tolist()
    return {
        "ema" : ema
    }