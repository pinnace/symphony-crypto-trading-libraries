from abc import ABC, abstractmethod
from typing import List, Union
from symphony.data_classes import Instrument
from symphony.enum import Exchange
from .client_abc import ClientABC
from typing import Optional
from .client_abc import ClientABC


class RealTimeQuoter(ABC):

    def __init__(self, log_level: Optional[int] = 0):
        self.symphony_client: ClientABC = None
        self.instruments: List[Instrument] = []
        self.exchange: Exchange = None

    @abstractmethod
    def contains_all_instruments(self) -> bool:
        pass

    @abstractmethod
    def get_bid(self, symbol_or_instrument: Union[Instrument, str], fall_back_to_api: Optional[bool] = False) -> float:
        pass

    @abstractmethod
    def get_ask(self, symbol_or_instrument: Union[Instrument, str], fall_back_to_api: Optional[bool] = False) -> float:
        pass

    @abstractmethod
    def get_bid_quantity(self, symbol_or_instrument: Union[Instrument, str], fall_back_to_api: Optional[bool] = False) -> float:
        pass

    @abstractmethod
    def get_ask_quantity(self, symbol_or_instrument: Union[Instrument, str], fall_back_to_api: Optional[bool] = False) -> float:
        pass

    @abstractmethod
    def get_midpoint(self, symbol_or_instrument: Union[Instrument, str], fall_back_to_api: Optional[bool] = False) -> float:
        pass

    @abstractmethod
    def get_liquidity(self, symbol_or_instrument: Union[Instrument, str], fall_back_to_api: Optional[bool] = False) -> float:
        pass


class HistoricalQuoter(ABC):

    def __init__(self, log_level: Optional[int] = 0):
        self.instruments: List[Instrument]
        self.exchange: Exchange