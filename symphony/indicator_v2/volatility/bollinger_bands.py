from symphony.data_classes import PriceHistory
from ..indicator_registry import IndicatorRegistry
import pandas_ta as ta
from typing import Optional


def bollinger_bands(price_history: PriceHistory, period: Optional[int] = 20, stdev: Optional[float] = 2.0, mamode: Optional[str] = "sma") -> PriceHistory:
    """
    Calculates Bollinger Bands, Width, and %

    :param price_history: Standard price history
    :param period: BB period
    :param stdev: Standard deviations
    :param mamode: Mamode, one of 'sma', 'ema'
    :return: Price history
    """

    df = price_history.price_history
    bb_df = df.ta.bbands(length=period, std=stdev, mamode=mamode)

    df[IndicatorRegistry.BOLLINGER_BANDS_LOWER.value] = bb_df[f"BBL_{str(period)}_{str(stdev)}"]
    df[IndicatorRegistry.BOLLINGER_BANDS_UPPER.value] = bb_df[f"BBU_{str(period)}_{str(stdev)}"]
    df[IndicatorRegistry.BOLLINGER_BANDS_WIDTH.value] = bb_df[f"BBB_{str(period)}_{str(stdev)}"]
    df[IndicatorRegistry.BOLLINGER_BANDS_PERCENT.value] = bb_df[f"BBP_{str(period)}_{str(stdev)}"]

    return price_history
