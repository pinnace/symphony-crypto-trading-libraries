from symphony.data_classes import PriceHistory
from ..indicator_registry import IndicatorRegistry
import pandas_ta as ta
from typing import Optional


def adx(price_history: PriceHistory, period: Optional[int] = 14, include_di: Optional[bool] = False) -> PriceHistory:
    """
    Calculates the ADX

    :param price_history: Standard price history
    :param period: Period
    :param include_di: Include +-DI in the dataframe
    :return: Price history
    """

    df = price_history.price_history
    adx_df = df.ta.adx(length=period)
    df[IndicatorRegistry.ADX.value] = adx_df[f"ADX_{str(period)}"]

    if include_di:
        df[IndicatorRegistry.PLUS_DI.value] = adx_df[f"DMP_{str(period)}"]
        df[IndicatorRegistry.MINUS_DI.value] = adx_df[f"DMN_{str(period)}"]
    return price_history
