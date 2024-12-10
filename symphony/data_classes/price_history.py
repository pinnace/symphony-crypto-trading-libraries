from typing import List, Dict, Union
from collections import OrderedDict
from dataclasses import dataclass
from symphony.enum import Timeframe
from .instrument import Instrument
from copy import deepcopy
from symphony.config import USE_MODIN

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

@dataclass
class PriceHistory:
    """
    PriceHistory object. Maintins a list of candles and their metadata.

    Args:
        instrument (str): The instrument to be traded
        timeframe (enum.Timeframe): Timeframe to be traded
        price_history (pandas.DataFrame): Price history dataframe

    """

    def __init__(self,
                 instrument: Instrument = None,
                 timeframe: Timeframe = None,
                 price_history: pd.DataFrame = []
                 ):
        self.instrument: Instrument = instrument
        self.timeframe: Timeframe = timeframe
        self.__internal_price_history_rep: OrderedDict[int, Dict[str, Union[float, pd.Timestamp]]] = {}
        self.price_history: pd.DataFrame = price_history

    @property
    def instrument(self) -> Instrument:
        return self.__instrument

    @instrument.setter
    def instrument(self, instrument: Instrument):
        self.__instrument = instrument

    @property
    def timeframe(self) -> Timeframe:
        return self.__timeframe

    @timeframe.setter
    def timeframe(self, timeframe: Timeframe):
        self.__timeframe = timeframe

    @property
    def price_history(self) -> pd.DataFrame:
        #return pd.DataFrame.from_dict(self.__internal_price_history_rep, "index")
        return self.__price_history

    @price_history.setter
    def price_history(self, price_history: pd.DataFrame):
        #self.__internal_price_history_rep = OrderedDict(price_history.to_dict("index"))
        self.__price_history = price_history

    def append(self, bar: Dict[pd.Timestamp, Dict[str, float]]) -> None:
        """
        Append a bar to the price history. Holds bars internally as OrderedDicts and creates DataFrames
        from them when needed. Surprisingly fast.

        Bar in form

        {Timestamp("...") { "open": .., "high": .., "low": .., "close": .., "volume": ..}}

        :param bar: Bar
        :return: None
        """
        for key in bar.keys():
            self.__internal_price_history_rep[key] = bar[key]
            for c in self.price_history.columns:
                if c not in bar[key].keys():
                    bar[key][c] = 0.0
            self.price_history.loc[key] = bar[key]
        return


def copy_price_history(price_history: PriceHistory) -> PriceHistory:
    """
    Returns a deep copy of the price history

    :param price_history: (`PriceHistory`) Price history to copy
    :return: (`PriceHistory`)
    """
    price_history_copy = deepcopy(price_history)
    price_history_copy.price_history = price_history.price_history.copy()
    return price_history_copy
