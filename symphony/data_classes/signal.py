from dataclasses import dataclass
from .instrument import Instrument
from symphony.enum import Timeframe, Market
from symphony.config import USE_MODIN
from symphony.exceptions import DataClassException

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


@dataclass
class Signal:

    def __init__(self):
        self.instrument: Instrument = None
        self.timeframe: Timeframe = None
        self.order_side: Market = None
        self.stop_loss: float = 0.0
        self.take_profit: float = 0.0
        self.order_id: str = ""
        self.timestamp: pd.Timestamp = None
        self.message: str = ""

    def __repr__(self):
        return f"{self.instrument.symbol} / {self.timeframe.name} / {self.order_side.value.upper()}"

    @property
    def instrument(self) -> Instrument:
        return self.__instrument

    @instrument.setter
    def instrument(self, instrument: Instrument):
        if not isinstance(instrument, Instrument) and not isinstance(instrument, type(None)):
            raise DataClassException(f"Parameter {instrument} must be of type instrument. Is type: {type(instrument)}")
        self.__instrument = instrument

    @property
    def timeframe(self) -> Timeframe:
        return self.__timeframe

    @timeframe.setter
    def timeframe(self, timeframe: Timeframe):
        if not isinstance(timeframe, Timeframe) and not isinstance(timeframe, type(None)):
            raise DataClassException(f"Parameter {timeframe} must be of type instrument")
        self.__timeframe = timeframe

    @property
    def order_side(self) -> Market:
        return self.__order_side

    @order_side.setter
    def order_side(self, order_side: Market):
        if not isinstance(order_side, type(None)):
            if order_side not in [Market.BUY, Market.SELL]:
                raise DataClassException(f"Order type must be Market type BUY or SELL: {order_side}")
        self.__order_side = order_side

    @property
    def stop_loss(self) -> float:
        return self.__stop_loss

    @stop_loss.setter
    def stop_loss(self, stop_loss: float):
        if not isinstance(stop_loss, float) and not isinstance(stop_loss, int):
            raise DataClassException(f"SL must be a float: {stop_loss}")
        self.__stop_loss = float(stop_loss)

    @property
    def take_profit(self) -> float:
        return self.__take_profit

    @take_profit.setter
    def take_profit(self, take_profit: float):
        if not isinstance(take_profit, float) and not isinstance(take_profit, int):
            raise DataClassException(f"TP must be a float: {take_profit}")
        self.__take_profit = float(take_profit)

    @property
    def order_id(self) -> str:
        return self.__order_id

    @order_id.setter
    def order_id(self, order_id: str):
        if not isinstance(order_id, str):
            raise DataClassException(f"Order id must be a str: {order_id}")
        self.__order_id = str(order_id)

    @property
    def timestamp(self) -> pd.Timestamp:
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, timestamp: pd.Timestamp):
        if not isinstance(timestamp, pd.Timestamp) and not isinstance(timestamp, type(None)):
            raise DataClassException(f"timestamp must be a Timestamp: {timestamp}")
        self.__timestamp = timestamp

    @property
    def message(self) -> str:
        return self.__message

    @message.setter
    def message(self, message: str):
        if not isinstance(message, str):
            raise DataClassException(f"message must be a str: {message}")
        self.__message = str(message)
