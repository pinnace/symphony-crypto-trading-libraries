
from enum import IntEnum, unique, auto
from typing import List


@unique
class IndicatorRegistry(IntEnum):
    """All indicators should be registered here

    The indicators are represented as integer constants,
        which will be used to index into price history + indicator
        matrices and standard price history objects
    
    """

    PRICE_HISTORY: int = 0
    
    # Indicator registry

    # Demark Indicators

    BULLISH_PRICE_FLIP: int = auto()
    BEARISH_PRICE_FLIP: int = auto()
    TD_BUY_SETUP: int = auto()
    TD_SELL_SETUP: int = auto()
    TD_BUY_COUNTDOWN: int = auto()
    TD_SELL_COUNTDOWN: int = auto()
    TD_COUNTDOWN: int = auto()
    
    # Moving Averages

    SMA: int = auto()
    EMA: int = auto()
    WMA: int = auto()

    # ZigZag

    ZIGZAG: int = auto()
    # End indicator registry

    







