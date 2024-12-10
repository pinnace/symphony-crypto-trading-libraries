from symphony.data_classes import PriceHistory
from ..indicator_registry import IndicatorRegistry
import pandas_ta as ta
from typing import Optional


def mass_index(price_history: PriceHistory, fast: Optional[int] = 9, slow: Optional[int] = 25) -> PriceHistory:
    """
    Calculates the Mass Index

    :param price_history: Standard price history
    :param fast: Fast period
    :param slow: Slow period
    :return: Price history
    """

    df = price_history.price_history
    df[IndicatorRegistry.MASS_INDEX.value] = df.ta.massi(fast=fast, slow=slow)
    return price_history
