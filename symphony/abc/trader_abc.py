from abc import ABC, abstractmethod
from .client_abc import ClientABC
from symphony.enum import Exchange, AccountType, Market
from symphony.data_classes import Instrument
from typing import List, Union, Optional, Any


class ExchangeTraderABC(ABC):

    def __init__(self):
        self.exchange_client: ClientABC
        self.exchange: Exchange

    @abstractmethod
    def transfer_isolated_margin_to_spot(self, asset: str, amount: float, symbol: str) -> None:
        pass

    @abstractmethod
    def transfer_spot_to_margin(self, asset: str, amount: float) -> None:
        pass

    @abstractmethod
    def transfer_spot_to_isolated_margin(self, asset: str, amount: float, symbol: str, **kwargs) -> None:
        pass

    @abstractmethod
    def transfer_margin_to_spot(self, asset: str, amount: float) -> None:
        pass

    @abstractmethod
    def transfer_isolated_margin_to_margin(self, asset: str, amount: float, symbol: str) -> None:
        pass

    @abstractmethod
    def transfer_isolated_margin_to_isolated_margin(self, asset: str, amount: float, from_symbol: str, to_symbol: str, **kwargs) -> None:
        pass

    @abstractmethod
    def transfer_margin_to_isolated_margin(self, asset: str, amount: float, symbol: str, **kwargs) -> None:
        pass

    @abstractmethod
    def transfer_any_to_spot(self, account: AccountType, asset: str, amount: float, **kwargs) -> None:
        pass

    @abstractmethod
    def transfer_spot_to_any(self, account: AccountType, asset: str, amount: float, **kwargs) -> None:
        pass

    @abstractmethod
    def execute_conversion_chain(self, conversion_chain: List[str], execution_chain: List[Market], initial_quantity: float, **kwargs) -> List[Any]:
        pass

    @abstractmethod
    def transfer(self, target_account: AccountType, target_asset: str, target_amount: float, **kwargs) -> float:
        pass

    @abstractmethod
    def round_lot(self, symbol_or_instrument: Union[Instrument], quantity: float, strategy: Optional[str] = str) -> float:
        pass

    @abstractmethod
    def market_order(self, symbol_or_instrument: Union[str, Instrument], account_type: AccountType, order_side: Market, quantity: float, client_order_id: Optional[str] = "",
                     **kwargs) -> Any:
        pass