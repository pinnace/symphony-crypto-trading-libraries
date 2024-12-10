from symphony.data_classes import PriceHistory
from ..indicator_registry import IndicatorRegistry
import pandas_ta as ta
from typing import Optional


def atr(price_history: PriceHistory, period: Optional[int] = 14, normalized: Optional[bool] = False) -> PriceHistory:
    """
    Calculates the Average True Range

    :param price_history: Standard price history
    :param period: ATR period
    :param normalized: Use NATR
    :return: Price history
    """

    df = price_history.price_history
    if normalized:
        df[IndicatorRegistry.NATR.value] = df.ta.natr(length=period)
    else:
        df[IndicatorRegistry.ATR.value] = df.ta.atr(length=period)

    return price_history
