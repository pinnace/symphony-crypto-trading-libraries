from symphony.data_classes import PriceHistory
from ..indicator_registry import IndicatorRegistry
import pandas_ta as ta
from typing import Optional


def sma(price_history: PriceHistory, period: Optional[int] = 20) -> PriceHistory:
    """
    Calculates SMA

    :param price_history: Standard price history
    :param period: Period
    :return: Price history
    """

    df = price_history.price_history
    sma_df = df.ta.sma(length=period)
    key = f"sma_{str(period)}"
    df[key] = sma_df
    return price_history
