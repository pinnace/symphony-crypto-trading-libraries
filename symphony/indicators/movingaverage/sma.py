from symphony.indicators.indicator_kit import IndicatorKit
from symphony.schema.schema_kit import schema
import talib
from numpy import isnan

@schema(filename="indicators/movingaverage/sma_schema.json")
def sma(flow: dict, period: int = 5) -> dict:
    """
    Simple moving average

    Args:
        flow (dict): Flow object
        period (int, optional): The SMA period. Defaults to 5

    Returns:
        (dict): SMA
    """
    sma = talib.SMA(IndicatorKit.get_points(flow, "close"), timeperiod=period)
    sma = sma[~isnan(sma)].tolist()
    return {
        "sma" : sma
    }