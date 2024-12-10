from symphony.config import USE_MODIN
from symphony.data_classes import PriceHistory
from ..indicator_registry import IndicatorRegistry
from ..indicator_kit import IndicatorKit
from symphony.enum import Column
from typing import Optional
import pandas_ta as ta

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

def td_waldo(price_history: PriceHistory, period: Optional[int] = 4) -> PriceHistory:
    """
    Calculates TD Waldo.

    :param price_history: Standard price history
    :param period: Do not change
    :return: Price history with indicator applied
    """

    df = price_history.price_history

    def calc_waldo(series: pd.Series) -> int:
        highs = df.loc[series.index, Column.HIGH]
        lows = df.loc[series.index, Column.LOW]
        closes = df.loc[series.index, Column.CLOSE]


        return 0

    df[IndicatorRegistry.TD_WALDO.value] = df[Column.HIGH].rolling(period + 1).apply(calc_waldo, raw=False)
    return price_history