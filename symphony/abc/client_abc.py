from abc import ABC, abstractmethod
from symphony.data_classes import PriceHistory, Instrument
from symphony.enum import Timeframe, Exchange
from typing import List, Union, Callable
import pandas as pd


class ClientABC(ABC):

    def __init__(self):
        self.ccxt_client: Callable
        self.instruments: List[Instrument]
        self.exchange: Exchange
        self.assets: List[str]

    @abstractmethod
    def get(self,
            instrument: Instrument,
            timeframe: Timeframe,
            num_bars: int,
            incomplete_bar: bool = False,
            end: pd.Timestamp = None) -> PriceHistory:
        """
        Get the most recent data at from a given start time to now (includes most recent, incomplete bar)

        :param instrument: The trading instrument
        :param timeframe: The timeframe we are trading
        :param num_bars: Look back `num_bars`
        :param incomplete_bar: True if we want to include the most recent uncompleted bar
        :param end: The last bar we want in the series
        :rtype: PriceHistory
        :raises ClientClassException: If the end bar and incomplete_bar are both specified
        """
        pass

    @abstractmethod
    def get_all_instruments(self) -> List[Instrument]:
        """
        Fetches a list of all instruments from this datasource

        :return: List of instrument objects
        :rtype: List[Instrument]
        """
        pass

    @abstractmethod
    def get_all_assets(self) -> List[str]:
        """
        Fetches a list of all assets from this datasource

        :return: List of assets
        :rtype: List[str]
        """
        pass

    @abstractmethod
    def get_multiple(self,
                     instruments: List[Instrument],
                     timeframes: Union[Timeframe, List[Timeframe]],
                     num_bars: int,
                     incomplete_bar: bool = False,
                     end: pd.Timestamp = None,
                     max_workers: int = 10
                     ) -> List[PriceHistory]:
        """
        Fetch multiple instruments in a parallel manner

        :param instruments: List of Instruments to fetch
        :param timeframes: List of timeframes
        :param num_bars: number of bars in each to get
        :param incomplete_bar: Whether or not to get the most recent (incomplete) bar, defaults to [False]
        :param end: Optional end index, defaults to [None]
        :param max_workers: Worker threads, defaults to [10]
        :return: List of PriceHistory objects
        """
        pass

