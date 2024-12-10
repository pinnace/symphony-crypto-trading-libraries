from symphony.config import USE_MODIN
from symphony.data_classes import PriceHistory
from ..indicator_kit import IndicatorKit
from symphony.indicator_v2.indicator_registry import IndicatorRegistry
from symphony.enum import Column
import numpy as np
from typing import Optional

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


def td_pressure(price_history: PriceHistory, period: Optional[int] = 5) -> PriceHistory:
    """
    Calculates TD Pressure

    :param price_history: Price history
    :param period: Default 5
    :return: Price history with indicator
    """

    df = price_history.price_history
    df[IndicatorRegistry.TD_PRESSURE.value] = np.nan

    def calc_pressure(series: pd.Series) -> float:

        opens = df.loc[series.index, Column.OPEN]
        closes = df.loc[series.index, Column.CLOSE]
        volumes = df.loc[series.index, Column.VOLUME]

        buy_pressure = 0
        sell_pressure = 0
        for i in range(-1, -1 - period, -1):
            delta = closes[i] - opens[i]
            true_range = IndicatorKit.get_true_range(price_history, i)
            if delta > 0:
                buy_pressure += (delta / true_range) * volumes[i]
            else:
                sell_pressure += (delta / true_range) * volumes[i]
        dominance = buy_pressure + abs(sell_pressure)
        if dominance:
            pressure = buy_pressure / dominance
        else:
            pressure = 0.5
        return pressure

    df[IndicatorRegistry.TD_PRESSURE.value] = df[Column.HIGH].rolling(period + 8).apply(calc_pressure, raw=False)
    return price_history