from symphony.data_classes import PriceHistory
from ..indicator_registry import IndicatorRegistry
from symphony.config import USE_MODIN
from symphony.enum import Column
import numpy as np
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


def bullish_price_flip(price_history: PriceHistory, window_size: int = 6) -> PriceHistory:
    """
    Calculates bullish priceflips on the PriceHistory data frame. Adds a new categorical column
    with name IndicatorRegistry.BULLISH_PRICE_FLIP

    :param price_history: The PriceHistory object
    :param window_size: Window size. Defaults to 6
    :return: Modified PriceHistory DataFrame inplace
    """
    df = price_history.price_history
    pf_series = pd.Series(np.zeros(len(df), dtype=int))

    for index in range(window_size - 1, len(pf_series)):
        assert (index - 5 >= 0)
        if df[Column.CLOSE].iloc[index] > df[Column.CLOSE].iloc[index - 4] and \
                df[Column.CLOSE].iloc[index - 1] < df[Column.CLOSE].iloc[index - 5]:
            pf_series[index] = 1
    df[IndicatorRegistry.BULLISH_PRICE_FLIP.value] = pf_series.values
    return price_history


def bearish_price_flip(price_history: PriceHistory, window_size: int = 6) -> PriceHistory:
    """
    Calculates bearish priceflips on the PriceHistory data frame. Adds a new categorical column
    with name IndicatorRegistry.BEARISH_PRICE_FLIP

    :param price_history: The PriceHistory object
    :param window_size: Window size. Defaults to 6
    :return: Modified price history
    """
    df = price_history.price_history
    pf_series = pd.Series(np.zeros(len(df), dtype=int))

    for index in range(window_size - 1, len(pf_series)):
        assert (index - 5 >= 0)
        if df[Column.CLOSE].iloc[index] < df[Column.CLOSE].iloc[index - 4] and \
                df[Column.CLOSE].iloc[index - 1] > df[Column.CLOSE].iloc[index - 5]:
            pf_series[index] = 1
    df[IndicatorRegistry.BEARISH_PRICE_FLIP.value] = pf_series.values
    return price_history

