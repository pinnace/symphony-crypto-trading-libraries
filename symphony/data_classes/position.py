from dataclasses import dataclass
from .instrument import Instrument
from .order import Order
from symphony.enum import Timeframe, Market, AccountType
from symphony.config import USE_MODIN
from symphony.exceptions import DataClassException
from typing import Optional

@dataclass
class Position:
    """
    Holds a representation of a position
    """

    def __init__(self,
                 instrument: Optional[Instrument] = None,
                 account_type: Optional[AccountType] = None,
                 position_id: Optional[str] = "",
                 side: Optional[Market] = None,
                 position_size: Optional[float] = 0.0,
                 borrow_txid: Optional[float] = 0.0,
                 borrow_amount: Optional[float] = 0.0,
                 borrow_denomination: Optional[str] = "",
                 margin_deposit: Optional[float] = 0.0,
                 deposit_denomination: Optional[str] = "",
                 margin_buy_amount: Optional[float] = 0.0,
                 stop_order: Optional[Order] = None,
                 timeframe: Optional[Timeframe] = None,
                 entry_value: Optional[float] = 0.0,
                 entry_denomination: Optional[str] = "",
                 profit: Optional[float] = 0.0,
                 exit_order: Optional[Order] = None
                 ):
        self.instrument = instrument
        self.account_type = account_type
        self.position_id = position_id
        self.side = side
        self.position_size = position_size
        self.borrow_txid = borrow_txid
        self.borrow_amount = borrow_amount
        self.borrow_denomination = borrow_denomination
        self.margin_deposit = margin_deposit
        self.deposit_denomination = deposit_denomination
        self.margin_buy_amount = margin_buy_amount
        self.stop_order = stop_order
        self.timeframe = timeframe
        self.entry_value = entry_value
        self.entry_denomination = entry_denomination
        self.profit = profit
        self.exit_order = exit_order
        return

    def __repr__(self):
        return f"Position / {self.instrument.symbol} / {self.account_type.value.upper()} / {self.side.value.upper()} / {self.position_size} {self.deposit_denomination}"

    @property
    def instrument(self) -> Instrument:
        return self.__instrument

    @instrument.setter
    def instrument(self, instrument: Instrument):
        if not isinstance(instrument, Instrument) and not isinstance(instrument, type(None)):
            raise DataClassException(f"Parameter {instrument} must be of type instrument. Is type: {type(instrument)}")
        self.__instrument = instrument

    @property
    def account_type(self) -> AccountType:
        return self.__account_type

    @account_type.setter
    def account_type(self, account_type: AccountType):
        self.__account_type = account_type

    @property
    def side(self) -> Market:
        return self.__side

    @side.setter
    def side(self, side: Market):
        self.__side = side

    @property
    def position_id(self) -> str:
        return self.__position_id

    @position_id.setter
    def position_id(self, position_id: ""):
        self.__position_id = position_id

    @property
    def position_size(self) -> float:
        return self.__position_size

    @position_size.setter
    def position_size(self, position_size: float):
        self.__position_size = position_size

    @property
    def borrow_txid(self) -> float:
        return self.__borrow_txid

    @borrow_txid.setter
    def borrow_txid(self, borrow_txid: float):
        self.__borrow_txid = borrow_txid

    @property
    def borrow_amount(self) -> float:
        return self.__borrow_amount

    @borrow_amount.setter
    def borrow_amount(self, borrow_amount: float):
        self.__borrow_amount = borrow_amount

    @property
    def borrow_denomination(self) -> str:
        return self.__borrow_denomination

    @borrow_denomination.setter
    def borrow_denomination(self, borrow_denomination: str):
        self.__borrow_denomination = borrow_denomination

    @property
    def deposit_denomination(self) -> str:
        return self.__deposit_denomination

    @deposit_denomination.setter
    def deposit_denomination(self, deposit_denomination: str):
        self.__deposit_denomination = deposit_denomination

    @property
    def margin_deposit(self) -> float:
        return self.__margin_deposit

    @margin_deposit.setter
    def margin_deposit(self, margin_deposit: float):
        self.__margin_deposit = margin_deposit

    @property
    def margin_buy_amount(self) -> float:
        return self.__margin_buy_amount

    @margin_buy_amount.setter
    def margin_buy_amount(self, margin_buy_amount: float):
        self.__margin_buy_amount = margin_buy_amount

    @property
    def stop_order(self) -> Order:
        return self.__stop_order

    @stop_order.setter
    def stop_order(self, stop_order: Order):
        self.__stop_order = stop_order

    @property
    def timeframe(self) -> Timeframe:
        return self.__timeframe

    @timeframe.setter
    def timeframe(self, timeframe: Timeframe):
        self.__timeframe = timeframe

    @property
    def entry_value(self) -> float:
        return self.__entry_value

    @entry_value.setter
    def entry_value(self, entry_value: float):
        self.__entry_value = entry_value

    @property
    def entry_denomination(self) -> str:
        return self.__entry_denomination

    @entry_denomination.setter
    def entry_denomination(self, entry_denomination: str):
        self.__entry_denomination = entry_denomination

    @property
    def profit(self) -> float:
        return self.__profit

    @profit.setter
    def profit(self, profit: float):
        self.__profit = profit

    @property
    def exit_order(self) -> Order:
        return self.__exit_order

    @exit_order.setter
    def exit_order(self, exit_order: Order):
        self.__exit_order = exit_order
