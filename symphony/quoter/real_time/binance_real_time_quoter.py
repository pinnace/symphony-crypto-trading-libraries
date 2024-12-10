from symphony.client import BinanceClient
from binance.client import Client
from binance.streams import ThreadedWebsocketManager
from symphony.borg import Borg
from symphony.data_classes import Instrument, PriceHistory
from symphony.utils.instruments import filter_instruments
from symphony.abc import RealTimeQuoter
from symphony.config import LOG_LEVEL
from symphony.utils.instruments import get_instrument
from symphony.enum import Column, Exchange, Timeframe, Market
from typing import Dict, List, Optional, Union
from symphony.exceptions import QuoterException
from twisted.internet import reactor
import logging

logger = logging.getLogger(__name__)


# TODO: Save tick data
# https://arctic.readthedocs.io/en/latest/
class BinanceRealTimeQuoter(RealTimeQuoter, Borg):

    def __init__(self, binance_client: BinanceClient, log_level: Optional[int] = LOG_LEVEL):
        """
        A class for providing real time quotes from Binance. Manages websockets

        :param binance_client: Binance client instance
        :param log_level: Optional log level
        """
        Borg.__init__(self)
        self.symphony_client: BinanceClient = binance_client
        self.client: Client = self.symphony_client.binance_client
        self.price_histories: Dict[Instrument, Dict[Timeframe, PriceHistory]] = {}
        self.__kline_conn_keys: Dict[Instrument, Dict[Timeframe, str]] = {}
        self.instruments: List[Instrument] = binance_client.get_all_instruments()
        self.all_symbols: List[str] = [instrument.symbol for instrument in self.instruments]
        self.socket_manager: ThreadedWebsocketManager = ThreadedWebsocketManager(api_key=self.symphony_client.api_key, api_secret=self.symphony_client.secret_key)
        self.socket_manager.start()
        self.__book_ticker_conn_key = self.__start_book_ticker()
        self.quotes: Dict[str, Dict[Column.BID, Column.ASK, Column.BID_QUANTITY, Column.ASK_QUANTITY]] = {}
        self.exchange: Exchange = Exchange.BINANCE
        logger.setLevel(log_level)

    def __del__(self):
        self.__stop_all()

    def stop(self) -> None:
        """
        Stops the quoter. WARNING: Stops reactor loop, which cannot be restarted. Can only be called ONCE
        per program run

        :return: None
        """
        self.__stop_all()

    def contains_all_instruments(self) -> bool:
        """
        Returns true if quoter contains information for all tradeable exchange instruments

        :return: True or False
        """
        logger.info(f"Quotes contains {len(self.quotes.keys())}/{len(self.all_symbols)} symbols")
        return all(symbol in self.quotes.keys() for symbol in self.all_symbols)

    def add_kline_websocket(self, symbol_or_instrument: Union[str, Instrument], timeframe: Timeframe, seed_bars: int = 100) -> None:
        """
        Starts a kline (candle) websocket for given instrument and time interval and maintains Pricehistory for it

        :param symbol_or_instrument:
        :param timeframe:
        :param seed_bars: Seed the price
        :return:
        """
        pass

    def get_bid(self, symbol_or_instrument: Union[Instrument, str], fall_back_to_api: Optional[bool] = False) -> float:
        """
        Get the bid price for a specific instrument or symbol

        :param symbol_or_instrument: Either instrument or symbol
        :param fall_back_to_api: If the websocket has not received a quote, fallback to querying API, defaults to [False]
        :return: The bid
        :raises QuoterException: If the symbol is not present and `fallback_to_api` is False
        """
        return self.__get(symbol_or_instrument, Column.BID, fall_back_to_api=fall_back_to_api)

    def get_ask(self, symbol_or_instrument: Union[Instrument, str], fall_back_to_api: Optional[bool] = False) -> float:
        """
        Get the ask price for a specific instrument or symbol

        :param symbol_or_instrument: Either instrument or symbol
        :param fall_back_to_api: If the websocket has not received a quote, fallback to querying API, defaults to [False]
        :return: The Ask
        :raises QuoterException: If the symbol is not present and `fallback_to_api` is False
        """
        return self.__get(symbol_or_instrument, Column.ASK, fall_back_to_api=fall_back_to_api)

    def get_bid_quantity(self, symbol_or_instrument: Union[Instrument, str], fall_back_to_api: Optional[bool] = False) -> float:
        """
        Get the bid quantity price for a specific instrument or symbol

        :param symbol_or_instrument: Either instrument or symbol
        :param fall_back_to_api: If the websocket has not received a quote, fallback to querying API, defaults to [False]
        :return: The bid quantity
        :raises QuoterException: If the symbol is not present and `fallback_to_api` is False
        """
        return self.__get(symbol_or_instrument, Column.BID_QUANTITY, fall_back_to_api=fall_back_to_api)

    def get_ask_quantity(self, symbol_or_instrument: Union[Instrument, str], fall_back_to_api: Optional[bool] = False) -> float:
        """
        Get the ask quantity for a specific instrument or symbol

        :param symbol_or_instrument: Either instrument or symbol
        :param fall_back_to_api: If the websocket has not received a quote, fallback to querying API, defaults to [False]
        :return: The Ask quantity
        :raises QuoterException: If the symbol is not present and `fallback_to_api` is False
        """
        return self.__get(symbol_or_instrument, Column.ASK_QUANTITY, fall_back_to_api=fall_back_to_api)

    def get_midpoint(self, symbol_or_instrument: Union[Instrument, str], fall_back_to_api: Optional[bool] = False) -> float:
        """
        Get the ask quantity for a specific instrument or symbol

        :param symbol_or_instrument: Either instrument or symbol
        :param fall_back_to_api: If the websocket has not received a quote, fallback to querying API, defaults to [False]
        :return: The Ask quantity
        :raises QuoterException: If the symbol is not present and `fallback_to_api` is False
        """
        return self.__get(symbol_or_instrument, Column.MIDPOINT, fall_back_to_api=fall_back_to_api)

    def get_liquidity(self, symbol_or_instrument: Union[Instrument, str], fall_back_to_api: Optional[bool] = False) -> float:
        """
        Gets crude liquidity. Ask - Bid / Ask

        :param symbol_or_instrument: Either instrument or symbol
        :param fall_back_to_api: If the websocket has not received a quote, fallback to querying API, defaults to [False]
        :return: Spread over price ratio
        :raises QuoterException: If the symbol is not present and `fallback_to_api` is False
        """
        return self.__get(symbol_or_instrument, Column.LIQUIDITY, fall_back_to_api=fall_back_to_api)

    def get_price(self, symbol_or_instrument: Union[Instrument, str], order_side: Market, fall_back_to_api: Optional[bool] = False) -> float:
        """
        Gets the right price, either BID or ASK, for the Market type

        :param symbol_or_instrument: Either instrument or symbol
        :param order_side: BUY or SELL
        :param fall_back_to_api: Optionally fall back to api
        :return: price
        :raises QuoterException: If the symbol is not present and `fallback_to_api` is False, if order_side is unknown
        """
        if order_side not in [Market.BUY, Market.SELL]:
            raise QuoterException(f"Unknown order side: {order_side}")

        if order_side == Market.BUY:
            return self.__get(symbol_or_instrument, Column.ASK, fall_back_to_api=fall_back_to_api)
        else:
            return self.__get(symbol_or_instrument, Column.BID, fall_back_to_api=fall_back_to_api)

    def __get_symbol(self, symbol_or_instrument: Union[Instrument, str]) -> str:
        """
        Helper method to extract a symbol

        :param symbol_or_instrument: Either instrument or symbol
        :return: The string representation
        """
        if isinstance(symbol_or_instrument, str):
            return symbol_or_instrument
        elif isinstance(symbol_or_instrument, Instrument):
            return symbol_or_instrument.symbol
        else:
            raise QuoterException(f"Unrecognized type: {type(symbol_or_instrument)} ")

    def __get(self, symbol_or_instrument: Union[Instrument, str], column: Column, fall_back_to_api: Optional[bool] = False) -> float:
        """
        Generic get for ASK, BID, ASK_QUANTITY, BID_QUANTITY, MIDPOINT, or LIQUIDITY

        :param symbol_or_instrument: Symbol or instrument
        :param column: Column
        :param fall_back_to_api: If the websocket has not received a quote, fallback to querying API, defaults to [False]
        :return: The quantity or price
        """
        instrument = get_instrument(self.instruments, symbol_or_instrument)

        symbol = instrument.symbol
        if symbol in self.quotes.keys():
            if column == Column.MIDPOINT:
                return round(
                    (self.quotes[symbol][Column.BID] + self.quotes[symbol][Column.ASK]) / 2, instrument.digits)
            elif column == Column.LIQUIDITY:
                return (self.quotes[symbol][Column.ASK] - self.quotes[symbol][Column.BID]) / self.quotes[symbol][Column.ASK]
            else:
                return self.quotes[symbol][column]
        elif fall_back_to_api:
            order_book = self.symphony_client.binance_client.get_order_book(symbol=symbol, limit=5)
            self.quotes[symbol] = {}
            self.quotes[symbol][Column.BID] = float(order_book["bids"][0][0])
            self.quotes[symbol][Column.BID_QUANTITY] = float(order_book["bids"][0][1])
            self.quotes[symbol][Column.ASK] = float(order_book["asks"][0][0])
            self.quotes[symbol][Column.ASK_QUANTITY] = float(order_book["asks"][0][1])
            return self.__get(symbol_or_instrument, column, fall_back_to_api=fall_back_to_api)
        else:
            raise QuoterException(f"Symbol {symbol} not present")

    def __start_book_ticker(self) -> str:
        """
        Starts the book ticker for all symbols

        :return: Connection key
        """
        return self.socket_manager.start_book_ticker_socket(self.__handle_incoming_book_ticker)

    def __stop_book_ticker(self) -> None:
        """
        Stops the book ticker

        :return:
        """
        self.socket_manager.stop_socket(self.__book_ticker_conn_key)
        self.__book_ticker_conn_key = None
        return

    def __stop_all(self) -> None:
        """
        This can only be called once

        :return: None
        """
        self.socket_manager.stop()
        reactor.stop()

    def __handle_incoming_book_ticker(self, message: Dict) -> None:
        """
        Handle websockets messages

        :param message: The message
        :return: None
        """
        if "e" in message.keys() and message["e"] == "error":
            if "m" in message.keys():
                logger.error(f"Binance book ticker error'd, restarting. Msg: {message['m']}")
            else:
                logger.error(f"Binance book ticker error'd, restarting.")
            self.__stop_book_ticker()
            self.__book_ticket_conn_key = self.__start_book_ticker()

        if "s" not in message.keys():
            raise QuoterException(f"Could not parse message: {message}")

        symbol = message["s"]
        if symbol not in self.quotes.keys():
            self.quotes[symbol] = {}
        # May not want all symbols. Use what is defined in client
        if symbol in self.all_symbols:
            self.quotes[symbol][Column.BID] = float(message["b"])
            self.quotes[symbol][Column.ASK] = float(message["a"])
            self.quotes[symbol][Column.BID_QUANTITY] = float(message["B"])
            self.quotes[symbol][Column.ASK_QUANTITY] = float(message["A"])
        return
