from symphony.data_classes import PriceHistory, Instrument
from symphony.abc import ClientABC
from symphony.enum import Timeframe, get_binance_client_timeframe, timeframe_to_numpy_string
from symphony.borg import Borg
from typing import List, Union, Generator, Optional, Dict, Any, Callable, Tuple
import concurrent.futures
import itertools
import cachetools.func
from functools import lru_cache
from time import sleep
import logging
from binance.client import Client
from binance.streams import ThreadedWebsocketManager
from concurrent.futures._base import ALL_COMPLETED
import requests
import json
import ccxt
from symphony.enum import Exchange
from symphony.utils.time import get_last_complete_bar_time, get_timestamp_of_num_bars_back, get_current_bar_open_time, \
    round_to_timeframe, get_num_bars_timestamp_to_present, get_num_bars_timestamp_to_timestamp, to_unix_time
from symphony.parser import BinanceParser, CCXTParser
from symphony.exceptions import ClientClassException
from symphony.config import config, LOG_LEVEL, USE_MODIN
from symphony.utils.misc import cartesian_product, grouper, chunker
from symphony.utils.instruments import get_instrument

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

logger = logging.getLogger(__name__)
anon_client = ccxt.binance()


class BinanceClient(ClientABC, Borg):
    """
    Binance client. Implements the Client Abstract Base Class. Is a Borg class, behaves like a singleton.
    """

    def __init__(self,
                 websocket_symbols: Union[str, Instrument, List[Union[str, Instrument]]] = None,
                 websocket_timeframes: Union[Timeframe, List[Timeframe]] = None,
                 price_history_seed: int = 100,
                 log_level: int = LOG_LEVEL
                 ):
        Borg.__init__(self)

        if websocket_symbols and not websocket_timeframes:
            raise ClientClassException(f"If websocket symbols, then must provide timeframes")

        self.api_key: str = config["client.binance"]["api_key"]
        self.secret_key: str = config["client.binance"]["api_secret"]
        self.binance_client = Client(self.api_key, self.secret_key)
        self.price_histories: Dict[str, Dict[Timeframe, PriceHistory]] = {}
        self.conn_keys: Dict[str, Dict[Timeframe, str]] = {}
        self.__websocket_settings: Dict[str, Dict[Timeframe, Dict[str, Any]]] = {}
        self.candle_websocket_callbacks: List[Callable] = []
        self.ccxt_client = ccxt.binance({
            'apiKey': self.api_key,
            'secret': self.secret_key,
            'timeout': 30000,
            'enableRateLimit': True,
        })
        self.ccxt_client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
        self.ccxt_client.load_markets()
        self.__headers = {'X-MBX-APIKEY': self.api_key}
        self.exchange = Exchange.BINANCE
        self.non_tradeable_assets: List[str] = []

        self.socket_manager = ThreadedWebsocketManager(api_key=self.api_key, api_secret=self.secret_key)
        self.socket_manager.start()

        self.websocket_instruments: List[Instrument] = []
        self.websocket_timeframes: Dict[str, List[Timeframe]] = {}
        self.__init_websockets(websocket_symbols, websocket_timeframes, price_history_seed)

        logger.setLevel(log_level)
        return

    def __init_websockets(self,
                          websocket_symbols: Union[str, Instrument, List[Union[str, Instrument]]],
                          websocket_timeframes: Union[Timeframe, List[Timeframe]],
                          price_history_seed: int
                          ) -> None:
        """
        Initialize websockets passed to class __init__

        :param websocket_symbols: User symbol or symbols
        :param websocket_timeframes: User timeframe or timeframes
        :param price_history_seed: Seed this number of bars
        :return: None
        """
        if price_history_seed and websocket_symbols:
            self.price_histories: Dict[str, Dict[Timeframe, PriceHistory]] = {}
            if isinstance(websocket_symbols, str):
                self.websocket_instruments = [get_instrument(self.instruments, websocket_symbols)]
            elif isinstance(websocket_symbols, Instrument):
                self.websocket_instruments = [websocket_symbols]
            elif isinstance(websocket_symbols, list):
                self.websocket_instruments = [get_instrument(self.instruments, symbol) for symbol in websocket_symbols]
            else:
                raise ClientClassException(f"Unknown type: {websocket_symbols}")

            if isinstance(websocket_timeframes, Timeframe):
                timeframes = [websocket_timeframes]
            elif isinstance(websocket_timeframes, list):
                for tf in websocket_timeframes:
                    if not isinstance(tf, Timeframe):
                        raise ClientClassException(f"Unknown type: {tf}, must be {type(Timeframe)}")
                timeframes = websocket_timeframes
            else:
                raise ClientClassException(f"Unknown type: {websocket_timeframes}, must be {type(Timeframe)}")

            for instrument in self.websocket_instruments:
                self.websocket_timeframes[instrument.symbol] = timeframes

            histories: List[PriceHistory] = \
                self.get_multiple(self.websocket_instruments, timeframes=timeframes,
                                  num_bars_or_start_time=price_history_seed)

            for history in histories:
                if history.instrument.symbol not in self.price_histories.keys():
                    self.price_histories[history.instrument.symbol] = {}
                self.price_histories[history.instrument.symbol][history.timeframe] = history
            for instrument in self.websocket_instruments:
                for timeframe in self.websocket_timeframes[instrument.symbol]:
                    self.start_candle_websocket(instrument, timeframe, price_history_seed=0)

            try:
                self.socket_manager.start()
            except RuntimeError:
                self.__socket_manager_initialized = True
            self.__socket_manager_initialized = True
        return

    def stop(self) -> None:
        """
        Stops the websockets for program exit

        :return: None
        """
        self.__stop_all()
        return

    @staticmethod
    def anon_get(instrument: Instrument,
                 timeframe: Timeframe,
                 num_bars_or_start_time: Union[int, pd.Timestamp],
                 incomplete_bar: Optional[bool] = False,
                 end: Optional[pd.Timestamp] = None
                 ) -> PriceHistory:
        """
        Gets OHCLV bars without using Binance API keys

        :param instrument: Instrument to fetch
        :param timeframe: Timeframe to fetch
        :param num_bars_or_start_time: Either the number of bars or the starting timestamp
        :param incomplete_bar: Whether or not to fetch the latest incomplete bar
        :param end: Optional desired end time
        :return: The PriceHistory object
        """
        start_bar_time, last_comp_bar_time = BinanceClient.__get_start_bar_time(timeframe, num_bars_or_start_time=num_bars_or_start_time, incomplete_bar=incomplete_bar, end=end)
        numpy_timeframe = timeframe_to_numpy_string(timeframe)
        if not isinstance(end, type(None)):
            num_bars = get_num_bars_timestamp_to_timestamp(start_bar_time, end, timeframe)
        else:
            num_bars = get_num_bars_timestamp_to_present(start_bar_time, timeframe, incomplete_bar=incomplete_bar)

        def get_df(ts: int) -> pd.DataFrame:
            candles = anon_client.fetch_ohlcv(instrument.base_asset + "/" + instrument.quote_asset, numpy_timeframe,
                                              since=ts)
            candles_df = CCXTParser.parse(candles)
            return candles_df

        def set_candles(candles: pd.DataFrame, candles_df: pd.DataFrame) -> pd.DataFrame:
            if isinstance(candles, type(None)):
                candles = candles_df
            else:
                candles = candles.append(candles_df)
            return candles

        full_cycles = num_bars // 500 # CCXT only fetches 500 bars at a time
        # timestamp in ms
        ts = to_unix_time(start_bar_time, resolution='ms')
        candles = None
        for cycle in range(full_cycles):
            candles_df = get_df(ts)
            candles = set_candles(candles, candles_df)
            ts = to_unix_time(candles.index[-1] + pd.Timedelta(numpy_timeframe), resolution='ms')

        leftover_bars = num_bars - (500 * full_cycles)
        if leftover_bars:
            candles_df = get_df(ts)
            candles = set_candles(candles, candles_df)


        if not incomplete_bar:
            candles = candles[:len(candles) - 1]

        return PriceHistory(instrument=instrument, timeframe=timeframe, price_history=candles)



    def get(self,
            instrument: Instrument,
            timeframe: Timeframe,
            num_bars_or_start_time: Union[int, pd.Timestamp],
            incomplete_bar: Optional[bool] = False,
            end: Optional[pd.Timestamp] = None
            ) -> PriceHistory:
        """
        Get the most recent data at from a given start time derived from `num_bars` to end time.
        Optionally include most recent, incomplete bar

        :param instrument: The trading instrument
        :param timeframe: The timeframe we are trading
        :param num_bars_or_start_time: Look back `num_bars` or start from a certain start time
        :param incomplete_bar: True if we want to include the most recent uncompleted bar. Do not use with [end]
        :param end: The last bar we want in the series. Do not use with [incomplete_bar]
        :return: Price History
        :raises ClientClassException: If the end bar and incomplete_bar are both specified
        """

        start_bar_time, last_comp_bar_time = BinanceClient.__get_start_bar_time(timeframe,
                                                                                num_bars_or_start_time=num_bars_or_start_time,
                                                                                incomplete_bar=incomplete_bar, end=end)
        candles = self.binance_client.get_historical_klines(
            instrument.symbol,
            get_binance_client_timeframe(timeframe),
            str(start_bar_time),
            end_str=str(last_comp_bar_time)
        )

        binance_df = BinanceParser.parse(candles)

        return PriceHistory(instrument=instrument, timeframe=timeframe, price_history=binance_df)

    # TODO Deprecate or change in favor of get_all_instruments
    @cachetools.func.ttl_cache(maxsize=128, ttl=43200)
    def get_all_symbols(self) -> List[Instrument]:
        """
        Fetches a list of all assets from this datasource

        :return: List of instrument objects
        :rtype: List[Instrument]
        """
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures.append(executor.submit(self.binance_client.get_exchange_info))
            futures.append(executor.submit(self.__get_all_margin_pairs))
            futures.append(executor.submit(self.__get_all_isolated_margin_pairs))
            futures.append(executor.submit(self.__get_isolated_margin_ratios_and_borrow_enabled))
            concurrent.futures.wait(futures, timeout=None, return_when=ALL_COMPLETED)
        info: Dict[str, Any] = futures[0].result()
        margin_pairs = futures[1].result()
        isolated_margin_pairs = futures[2].result()
        isolated_margin_ratios, borrow_enabled = futures[3].result()
        instruments = []
        potential_non_ta = []
        for symbol in info["symbols"]:
            if symbol["status"] != "TRADING":
                potential_non_ta.append(symbol["baseAsset"])
                potential_non_ta.append(symbol["quoteAsset"])
                continue
            instrument = Instrument()
            instrument.symbol = symbol["symbol"]
            instrument.digits = symbol["quotePrecision"]
            instrument.exchange = Exchange.BINANCE
            instrument.is_currency = True
            instrument.base_asset = symbol["baseAsset"]
            instrument.quote_asset = symbol["quoteAsset"]
            instrument.margin_allowed = symbol["symbol"] in margin_pairs
            instrument.isolated_margin_allowed = symbol["symbol"] in isolated_margin_pairs
            # Try to set isolated margin ratio. If we can't something went wrong and we shouldnt trade this
            if instrument.isolated_margin_allowed:
                if instrument.symbol in isolated_margin_ratios.keys():
                    if not borrow_enabled[instrument.symbol]:
                        instrument.isolated_margin_allowed = False
                    else:
                        instrument.isolated_margin_ratio = isolated_margin_ratios[instrument.symbol]
                        instrument.isolated_margin_account_created = True
                else:
                    instrument.isolated_margin_account_created = False

            instrument.oco_allowed = symbol['ocoAllowed']
            for exchange_filter in symbol['filters']:
                if exchange_filter['filterType'] == 'LOT_SIZE':
                    ccxt_symbol = instrument.base_asset + "/" + instrument.quote_asset
                    if ccxt_symbol in self.ccxt_client.markets.keys():
                        instrument.min_quantity = self.ccxt_client.markets[ccxt_symbol]['limits']['cost']['min']
                    else:
                        instrument.min_quantity = float(exchange_filter['minQty'])
                    instrument.max_quantity = float(exchange_filter['maxQty'])
                    instrument.step_size = float(exchange_filter['stepSize'])
            instruments.append(instrument)

        # Gather all non tradeable assets
        all_assets = []
        for instrument in instruments:
            all_assets.append(instrument.base_asset)
            all_assets.append(instrument.quote_asset)
        all_assets = list(set(all_assets))

        for pnta in potential_non_ta:
            if pnta not in all_assets:
                self.non_tradeable_assets.append(pnta)

        return instruments

    def get_all_instruments(self) -> List[Instrument]:
        """
        New standardized naming for get_all_symbols()

        :return:
        """
        return self.get_all_symbols()

    @lru_cache
    def get_all_assets(self) -> List[str]:
        """
        Gets all supported assets on platform (e.g. 'BTC', 'EUR')

        :return: List of all supported assets
        """
        assets = []
        for instrument in self.get_all_instruments():
            assets.append(instrument.base_asset)
            assets.append(instrument.quote_asset)
        return list(set(assets))

    @lru_cache
    def get_all_margin_assets(self) -> List[str]:
        """
        Gets all supported margin assets on platform (e.g. 'BTC', 'EUR')

        :return: List of all supported assets
        """
        assets = []
        for instrument in self.get_all_instruments():
            if instrument.margin_allowed:
                assets.append(instrument.base_asset)
                assets.append(instrument.quote_asset)
        return list(set(assets))

    def get_multiple(self,
                     instruments: List[Instrument],
                     timeframes: Union[Timeframe, List[Timeframe]],
                     num_bars_or_start_time: Union[int, pd.Timestamp],
                     incomplete_bar: Optional[bool] = False,
                     end: pd.Timestamp = None,
                     filter_exchange: Optional[bool] = True,
                     fail_on_exception: Optional[bool] = False,
                     max_workers: Optional[int] = 10,
                     sleep_time_secs: Optional[int] = 1.5
                     ) -> List[PriceHistory]:
        """
        Fetch multiple instruments in a parallel manner

        :param instruments: List of Instruments to fetch
        :param timeframes: Single or list of timeframes
        :param num_bars_or_start_time: Look back `num_bars` or start from a certain start time
        :param incomplete_bar: Whether or not to get the most recent (incomplete) bar. Do not use with `end`.
                                Defaults to [False]
        :param end: Optional end index. Do not use with `incomplete_bar`. Defaults to [None]
        :param filter_exchange: Optionally filter the instruments by exchange
        :param fail_on_exception: If True, raise an error if API exceptions are detected.
                                    Otherwise exclude from return results
        :param max_workers: Worker threads, as well as chunk size, defaults to [10]
        :param sleep_time_secs: Sleep time inbetween API calls of chunk size. Default is Pretuned [1.5s].
        :return: List of PriceHistory objects
        :raises ClientClassException: If we want to fail on exception, if no data was able to be obtained
        """
        if filter_exchange:
            instruments = list(filter(lambda instrument: instrument.exchange == Exchange.BINANCE, instruments))

        combinations: List[tuple] = cartesian_product(instruments, timeframes)
        chunks: Generator[List[tuple], None, None] = chunker(combinations, max_workers)

        # Could get 817 symbols before exceptions
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            all_futures: List[concurrent.Future] = []
            for chunk in chunks:
                chunk_futures = [
                    executor.submit(
                        self.get,
                        instrument,
                        timeframe,
                        num_bars_or_start_time,
                        incomplete_bar=incomplete_bar,
                        end=end)
                    for instrument, timeframe in chunk
                ]
                all_futures.append(chunk_futures)
                sleep(sleep_time_secs)
            futures: List[concurrent.Future] = list(itertools.chain.from_iterable(all_futures))
        price_histories: List[PriceHistory] = [
            future.result() for future in futures if isinstance(future.exception(), type(None))
        ]
        exceptions: List[Exception] = [
            future.exception() for future in futures if not isinstance(future.exception(), type(None))
        ]

        if len(exceptions):
            if fail_on_exception:
                raise ClientClassException(f"Exceptions were detected. "
                                           f"Detected {len(exceptions)}/{len(combinations)} as exceptions. "
                                           f"Example exception: {exceptions[0]}")
            logger.info(__name__ + f" [-] Detected {len(exceptions)}/{len(combinations)} as exceptions")

        if not (len(price_histories)):
            raise ClientClassException(f"Failed to retrieve any data")

        return price_histories

    @property
    def instruments(self) -> List[Instrument]:
        return self.get_all_instruments()

    def start_candle_websocket(self,
                               symbol_or_instrument: Union[str, Instrument],
                               timeframe: Timeframe,
                               incomplete_bars: Optional[bool] = False,
                               websocket_callback: Optional[Callable] = None,
                               price_history_seed: Optional[int] = 100
                               ) -> None:
        """
        Start the handler for a kline websocket

        :param symbol_or_instrument: Symbol of instrument
        :param timeframe: Timeframe
        :param incomplete_bars: Whether to process and append partial bars to price history
        :param websocket_callback: Optional callback to register. Can also use register_websocket_callback
        :param price_history_seed: Seed PriceHistory with this number of bars. Also passes incomplete_bars.
        :return: None
        """
        instrument = get_instrument(self.instruments, symbol_or_instrument)
        logger.info(f"Registering kline websocket for {instrument}")

        if instrument.symbol not in self.conn_keys.keys():
            self.conn_keys[instrument.symbol] = {}
            self.__websocket_settings[instrument.symbol] = {}
        if instrument.symbol not in self.price_histories.keys():
            self.price_histories[instrument.symbol] = {}
        if timeframe not in self.price_histories[instrument.symbol].keys():
            self.price_histories[instrument.symbol][timeframe] = None

        self.__websocket_settings[instrument.symbol][timeframe] = {
            "incomplete": incomplete_bars
        }

        if websocket_callback and websocket_callback not in self.candle_websocket_callbacks:
            self.candle_websocket_callbacks.append(websocket_callback)

        if instrument not in self.websocket_instruments:
            self.websocket_instruments.append(instrument)
        if instrument.symbol in self.websocket_timeframes.keys():
            if timeframe not in self.websocket_timeframes[instrument.symbol]:
                self.websocket_timeframes[instrument.symbol].append(timeframe)
        else:
            self.websocket_timeframes[instrument.symbol] = [timeframe]

        handler: Callable = self.__kline_function_template(instrument.symbol, timeframe)
        binance_tf = get_binance_client_timeframe(timeframe)

        if timeframe in self.conn_keys[instrument.symbol] and self.conn_keys[instrument.symbol][timeframe]:
            pass
        else:
            self.conn_keys[instrument.symbol][timeframe] = self.socket_manager.start_kline_socket(handler, instrument.symbol, interval=binance_tf)
            if price_history_seed:
                self.price_histories[instrument.symbol][timeframe] = self.get(instrument, timeframe,
                                                                              num_bars_or_start_time=price_history_seed,
                                                                              incomplete_bar=incomplete_bars)

        return

    def stop_candle_websocket(self, symbol_or_instrument: Union[str, Instrument], timeframe: Timeframe) -> None:
        """
        Stops a kline socket and remove from class variables

        :param symbol_or_instrument: Symbol or instrument to stop
        :param timeframe: Timeframe to stop
        :return: None
        """
        instrument = get_instrument(self.instruments, symbol_or_instrument)
        if instrument.symbol in self.conn_keys.keys():
            if timeframe in self.conn_keys[instrument.symbol].keys():
                self.socket_manager.stop_socket(self.conn_keys[instrument.symbol][timeframe])
                del self.conn_keys[instrument.symbol][timeframe]
                del self.__websocket_settings[instrument.symbol][timeframe]
                if self.websocket_timeframes[instrument.symbol]:
                    if timeframe in self.websocket_timeframes[instrument.symbol]:
                        if len(self.websocket_timeframes[instrument.symbol]) == 1:
                            del self.websocket_timeframes[instrument.symbol]
                            inst_index = self.websocket_instruments.index(instrument)
                            del self.websocket_instruments[inst_index]
                        else:
                            index = self.websocket_timeframes[instrument.symbol].index(timeframe)
                            del self.websocket_timeframes[instrument.symbol][index]

        return

    def register_websocket_callback(self, callback: Callable) -> None:
        """
        Registers a candlestick callback if not present

        :param callback: Callback function
        :return: None
        """
        if callback not in self.candle_websocket_callbacks:
            self.candle_websocket_callbacks.append(callback)
        return

    def deregister_websocket_callback(self, callback: Callable) -> None:
        """
        Removes a callback if present

        :param callback: Callback function
        :return: None
        """

        if callback in self.candle_websocket_callbacks:
            index = self.candle_websocket_callbacks.index(callback)
            del self.candle_websocket_callbacks[index]
        return

    @staticmethod
    def __get_start_bar_time(timeframe: Timeframe,
                             num_bars_or_start_time: Union[int, pd.Timestamp],
                             incomplete_bar: Optional[bool] = False,
                             end: Optional[pd.Timestamp] = None) -> Tuple[pd.Timestamp, pd.Timestamp]:
        """
        Helper method to get the start time of the requested series of candles

        :param timeframe: Timeframe
        :param num_bars_or_start_time: Either the number of bars desired or the start date
        :param incomplete_bar: Whether or not to fetch incomplete bar
        :param end: The optional desired end date
        :return: The timestamp
        """
        # If a specific end bar is defined, we don't need to request `incomplete_bar`. This
        # would likely be a logical error
        if end and incomplete_bar:
            raise ClientClassException(f"Do not need to specify both and and incomplete_bar. End: {str(end)}")
        # Sanity check for start and end times
        if end and isinstance(num_bars_or_start_time, pd.Timestamp):
            if num_bars_or_start_time > end:
                raise ClientClassException(f"Start time is a timestamp and it is greater than end. "
                                           f"Start: {str(num_bars_or_start_time)}, End: {str(end)}")
        # If a specific end time is not specified, use current time.
        # In binance client API start time refers to earliest bar, `end` bar
        # refers to more recent bar
        if not incomplete_bar:
            if end:
                last_comp_bar_time: pd.Timestamp = end
            else:
                last_comp_bar_time: pd.Timestamp = get_last_complete_bar_time(timeframe)
        if incomplete_bar:
            last_comp_bar_time: pd.Timestamp = get_current_bar_open_time(timeframe)

        if isinstance(num_bars_or_start_time, int):
            start_bar_time: pd.Timestamp = get_timestamp_of_num_bars_back(timeframe, last_comp_bar_time,
                                                                          num_bars_or_start_time)
        elif isinstance(num_bars_or_start_time, pd.Timestamp):
            start_bar_time: pd.Timestamp = round_to_timeframe(num_bars_or_start_time, timeframe)
        else:
            raise ClientClassException(f"Unknown type: {type(num_bars_or_start_time)}")
        return start_bar_time, last_comp_bar_time

    def __handle_kline_event(self, msg: Dict[str, Dict[str, str]], symbol: str, timeframe: Timeframe) -> None:
        """
        Handler for kline events

        :param msg: Websocket message
        :param symbol: For symbol
        :param timeframe: For timeframe
        :return: None
        """
        instrument = get_instrument(self.instruments, symbol)
        if instrument not in self.websocket_instruments:
            raise ClientClassException(f"Symbol {symbol} not configured for websocket")
        if timeframe not in self.websocket_timeframes[symbol]:
            raise ClientClassException(f"Timeframe {timeframe} not configured for {symbol} websocket")

        if symbol not in self.price_histories.keys():
            raise ClientClassException(f"Symbol {symbol} not configured for price history")

        def call_callbacks(price_history: PriceHistory) -> None:
            for callback in self.candle_websocket_callbacks:
                callback(price_history)
            return

        row = {}
        if self.__websocket_settings[symbol][timeframe]["incomplete"]:
            row = BinanceParser.parse_websocket_message(msg)
        elif msg["k"]["x"]:
            row = BinanceParser.parse_websocket_message(msg)
        else:
            pass

        if row:
            print(f"{symbol} / {timeframe} {row}")
            self.price_histories[symbol][timeframe].append(row)
            call_callbacks(self.price_histories[symbol][timeframe])

        return

    def __kline_function_template(self, symbol: str, timeframe: Timeframe) -> Callable:
        def kline_handler(msg):
            self.__handle_kline_event(msg, symbol, timeframe)
            return

        kline_handler.__name__ = f"__kline_handler_{symbol}{str(timeframe.value)}"
        return kline_handler

    def __get_all_margin_pairs(self) -> List[str]:
        """
        Fetches all margin pairs using raw API request

        :return: List of margin symbols
        :raises ClientClassException: For API error
        """
        resp = requests.get(self.binance_client.MARGIN_API_URL + "/v1/margin/allPairs", headers=self.__headers)
        if resp.status_code == 200:
            data = json.loads(resp.text)
            return [pair["symbol"] for pair in data]
        else:
            raise ClientClassException(f"Failed to request all margin pairs. Resp: [{resp.status_code}] {resp.text}")

    def __get_all_isolated_margin_pairs(self) -> List[str]:
        """
        Gets all isolated margin pairs

        :return: List of pairs
        """
        return [i["symbol"] for i in self.binance_client.get_all_isolated_margin_symbols()]

    def __get_isolated_margin_ratios_and_borrow_enabled(self) -> Tuple[Dict[str, int], Dict[str, bool]]:
        """
        Fetches isolated margin ratios

        :return: Mapping of symbols to margin ratio
        """
        iso_acct = self.binance_client.get_isolated_margin_account()
        ratios: Dict[str, int] = {}
        borrow_enabled: Dict[str, int] = {}
        assets = iso_acct['assets']
        for asset in assets:
            ratios[asset['symbol']] = int(asset['marginRatio'])
            borrow_enabled[asset['symbol']] = asset['baseAsset']['borrowEnabled'] | asset['quoteAsset']['borrowEnabled']
        return ratios, borrow_enabled

    def __stop_all(self) -> None:
        """
        This can only be called once!

        :return: None
        """
        self.socket_manager.close()
        return
