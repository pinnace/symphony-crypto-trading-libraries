from dataclasses import dataclass
from .instrument import Instrument
from symphony.enum import Exchange, Market, OrderStatus, AccountType
from symphony.exceptions import DataClassException
from typing import Optional, Union, Dict
from symphony.config import USE_MODIN

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


@dataclass
class Order:

    def __init__(self,
                 instrument: Optional[Instrument] = None,
                 status: Optional[OrderStatus] = None,
                 timestamp: Optional[pd.Timestamp] = None,
                 last_traded_timestamp: Optional[Union[pd.Timestamp, None]] = None,
                 order_id: Optional[int] = -1,
                 client_order_id: Optional[str] = "",
                 exchange: Optional[Exchange] = None,
                 account: Optional[AccountType] = None,
                 commission_amount: Optional[float] = 0.0,
                 commission_asset: Optional[str] = "",
                 price: Optional[float] = 0.0,
                 quantity: Optional[float] = 0.0,
                 transacted_quantity: Optional[float] = 0.0,
                 filled: Optional[float] = 0.0,
                 order_side: Optional[Market] = None,
                 order_type: Optional[Market] = None,
                 stop_price: Optional[float] = -1.0
                 ):
        """
        Order data class

        :param instrument: Instrument being traded
        :param status: Trading status (e.g. OPEN, FILLED, CANCELLED)
        :param timestamp: order placed time as pandas Timestamp
        :param last_traded_timestamp: Time of last trade
        :param order_id: Unique order id
        :param client_order_id: User provided unique order id
        :param exchange: Exchange enum
        :param account: Order account type [SPOT, MARGIN, ISOLATED_MARGIN]
        :param commission_amount: Commission paid
        :param commission_asset: Commission denominations
        :param price: Order price
        :param quantity: Order quantity
        :param transacted_quantity: Amount received in counter currency
        :param filled: Order quantity filled
        :param order_side: BUY or SELL
        :param order_type: One of OrderType
        :param stop_price: Stop price
        """

        self.instrument: Instrument = instrument
        self.status: OrderStatus = status
        self.timestamp: pd.Timestamp = timestamp
        self.last_traded_timestamp: pd.Timestamp = last_traded_timestamp
        self.exchange: Exchange = exchange
        self.account: AccountType = account
        self.commission_amount: float = commission_amount
        self.commission_asset: str = commission_asset
        self.order_id: int = order_id
        self.client_order_id: str = client_order_id
        self.price: float = price
        self.quantity: float = quantity
        self.transacted_quantity: float = transacted_quantity
        self.filled: float = filled
        self.order_side: Market = order_side
        self.order_type: Market = order_type
        self.stop_price: float = stop_price

    def __repr__(self):
        return f"{self.exchange.name.upper()} ID{{{self.order_id}}} {self.status.value.upper()} / {self.order_side.value.upper()} / {self.order_type.value.upper()} / " \
               f"{self.instrument.symbol} / {self.quantity} @ {'MARKET' if not self.price else self.price}"

    def __eq__(self, other):
        if not isinstance(other, Order):
            return NotImplemented

        return self.order_id == other.order_id and self.order_side == other.order_side and self.exchange == other.exchange and \
               self.order_type == other.order_type and self.price == other.price and self.status == other.status and \
               self.instrument == other.instrument and self.quantity == other.quantity and self.price == other.price and \
               self.transacted_quantity == other.transacted_quantity

    @property
    def instrument(self) -> Instrument:
        return self.__instrument

    @instrument.setter
    def instrument(self, instrument: Instrument):
        if not isinstance(instrument, Instrument) and not isinstance(instrument, type(None)):
            raise DataClassException(f"Parameter {instrument} must be of type instrument")
        self.__instrument = instrument

    @property
    def status(self) -> OrderStatus:
        return self.__status

    @status.setter
    def status(self, status: OrderStatus):
        if not isinstance(status, type(None)) and status not in OrderStatus:
            raise DataClassException(f"status {status} is not a valid OrderStatus")
        self.__status = status

    @property
    def timestamp(self) -> pd.Timestamp:
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, timestamp: pd.Timestamp):
        if not isinstance(timestamp, pd.Timestamp) and not isinstance(timestamp, type(None)):
            raise DataClassException(f"Parameter {timestamp} must be of type pd.Timestamp, provided: {type(timestamp)}")
        self.__timestamp = timestamp

    @property
    def last_traded_timestamp(self) -> Union[pd.Timestamp, None]:
        return self.__last_traded_timestamp

    @last_traded_timestamp.setter
    def last_traded_timestamp(self, last_traded_timestamp: Union[pd.Timestamp, None]):
        if not isinstance(last_traded_timestamp, pd.Timestamp) and not isinstance(last_traded_timestamp, type(None)):
            raise DataClassException(f"Parameter {last_traded_timestamp} must be of type pd.Timestamp, provided: {type(last_traded_timestamp)}")
        self.__last_traded_timestamp = last_traded_timestamp

    @property
    def order_id(self) -> int:
        return self.__order_id

    @order_id.setter
    def order_id(self, order_id: int):
        if not isinstance(order_id, int):
            raise DataClassException(f"Order id must be an int: {order_id}")
        self.__order_id = order_id

    @property
    def client_order_id(self) -> str:
        return self.__client_order_id

    @client_order_id.setter
    def client_order_id(self, client_order_id: str):
        if not isinstance(client_order_id, str):
            raise DataClassException(f"Client order id must be an string: {client_order_id}")
        self.__client_order_id = client_order_id

    @property
    def exchange(self) -> Exchange:
        return self.__exchange

    @exchange.setter
    def exchange(self, exchange: Exchange):
        if not isinstance(exchange, Exchange) and not isinstance(exchange, type(None)):
            raise DataClassException(f"Exchange not recognized: {exchange}")
        self.__exchange = exchange

    @property
    def account(self) -> AccountType:
        return self.__account

    @account.setter
    def account(self, account: AccountType):
        if not isinstance(account, AccountType) and not isinstance(account, type(None)):
            raise DataClassException(f"AccountType not recognized: {account}")
        self.__account = account

    @property
    def commission_amount(self) -> float:
        return self.__commission_amount

    @commission_amount.setter
    def commission_amount(self, commission_amount: float):
        if not isinstance(commission_amount, float) and not isinstance(commission_amount, int):
            raise DataClassException(f"commission_amount must be a float: {commission_amount}")
        self.__commission_amount = float(commission_amount)

    @property
    def commission_asset(self) -> str:
        return self.__commission_asset

    @commission_asset.setter
    def commission_asset(self, commission_asset: str):
        if not isinstance(commission_asset, str) and not isinstance(commission_asset, type(None)):
            raise DataClassException(f"commission_asset must be a str: {commission_asset}")
        self.__commission_asset = commission_asset

    @property
    def order_type(self) -> Market:
        return self.__order_type

    @order_type.setter
    def order_type(self, order_type: Market):
        if not isinstance(order_type, type(None)):
            if order_type not in Market:
                raise DataClassException(f"Order type must be Market type: {order_type}")
        self.__order_type = order_type

    @property
    def order_side(self) -> Market:
        return self.__order_side

    @order_side.setter
    def order_side(self, order_side: Market):
        if order_side != Market.BUY and order_side != Market.SELL and not isinstance(order_side, type(None)):
            raise DataClassException(f"Order side must be a [BUY, SELL] type: {order_side}")
        self.__order_side = order_side

    @property
    def price(self) -> float:
        return self.__price

    @price.setter
    def price(self, price: float):
        if not isinstance(price, float) and not isinstance(price, int):
            raise DataClassException(f"Price must be a float: {price}")
        self.__price = float(price)

    @property
    def quantity(self) -> float:
        return self.__quantity

    @quantity.setter
    def quantity(self, quantity: float):
        if not isinstance(quantity, float) and not isinstance(quantity, int):
            raise DataClassException(f"Quantity must be a float: {quantity}")
        self.__quantity = float(quantity)

    @property
    def transacted_quantity(self) -> float:
        return self.__transacted_quantity

    @transacted_quantity.setter
    def transacted_quantity(self, transacted_quantity: float):
        if not isinstance(transacted_quantity, float) and not isinstance(transacted_quantity, int):
            raise DataClassException(f"Transacted Quantity must be a float: {transacted_quantity}")
        self.__transacted_quantity = float(transacted_quantity)

    @property
    def filled(self) -> float:
        return self.__filled

    @filled.setter
    def filled(self, filled: float):
        if not isinstance(filled, float) and not isinstance(filled, int):
            raise DataClassException(f"Filled quantity must be a float: {filled}")
        self.__filled = float(filled)

    @property
    def stop_price(self) -> float:
        return self.__stop_price

    @stop_price.setter
    def stop_price(self, stop_price: float):
        if not isinstance(stop_price, float) and not isinstance(stop_price, int):
            raise DataClassException(f"Stop price must be a float: {stop_price}")
        self.__stop_price = float(stop_price)


