from abc import ABC, abstractmethod
from typing import List, Optional, Union
from symphony.enum import Timeframe
from symphony.data_classes import Instrument
from symphony.config import LOG_LEVEL
import pandas


class ArchiverABC(ABC):

    @abstractmethod
    def __init__(self, save_location: str, use_s3: Optional[bool] = False, log_level: Optional[int] = LOG_LEVEL):
        pass

    @abstractmethod
    def save(self, instrument: Instrument, timeframe: Timeframe) -> None:
        pass

    @abstractmethod
    def update(self, instrument: Instrument, timeframe: Timeframe) -> None:
        pass

    @abstractmethod
    def save_and_update(self, instrument: Instrument, timeframe: Timeframe) -> None:
        pass

    @abstractmethod
    def save_multiple(self,
                      instruments: Optional[List[Instrument]] = None,
                      timeframes: Optional[Union[Timeframe, List[Timeframe]]] = None
                      ) -> None:
        pass

    @abstractmethod
    def update_multiple(self,
                        instruments: Optional[List[Instrument]] = None,
                        timeframes: Optional[Union[Timeframe, List[Timeframe]]] = None
                        ) -> None:
        pass

    @abstractmethod
    def save_and_update_multiple(self,
                                 instruments: Optional[List[Instrument]] = None,
                                 timeframes: Optional[Union[Timeframe, List[Timeframe]]] = None
                                 ) -> None:
        pass


