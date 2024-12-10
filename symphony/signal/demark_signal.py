from symphony.abc import RealTimeQuoter, ClientABC
from symphony.client import BinanceClient
from typing import List, Union, Optional, Dict
from symphony.data_classes import Instrument, PriceHistory, Signal, Order, Position
from symphony.enum import Timeframe, Market, AccountType
from symphony.execution import BinanceTrader
from symphony.account import BinanceAccountManager
from symphony.exceptions import SignalException
from symphony.utils.instruments import get_instrument, filter_instruments
from symphony.utils.time import to_unix_time
from symphony.indicator_v2.demark import td_upwave, td_downwave, td_buy_setup, td_sell_setup, td_buy_countdown, td_sell_countdown, td_buy_9_13_9, td_sell_9_13_9, \
    bullish_price_flip, bearish_price_flip, td_buy_combo, td_sell_combo
from symphony.indicator_v2.demark.helpers import td_stoploss, get_string_rep_short
from symphony.indicator_v2.indicator_registry import IndicatorRegistry
from symphony.config import LOG_LEVEL, USE_MODIN
import logging
import base64

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

logger = logging.getLogger(__name__)

class DemarkSignal:

    def __init__(self,
                 trader: BinanceTrader,
                 symbols_or_instruments: List[Union[str, Instrument]],
                 timeframes: Union[Timeframe, List[Timeframe]],
                 incomplete_bars: Optional[bool] = False,
                 risk_perc: Optional[float] = 0.02,
                 account_denomination: Optional[str] = "EUR",
                 margin: Optional[bool] = False,
                 minimum_margin_level: Optional[float] = 1.3,
                 trade_signals: Optional[bool] = False,
                 price_history_seed: Optional[int] = 300,
                 bootstrap_price_history: Optional[bool] = True,
                 log_level: Optional[int] = LOG_LEVEL
                 ):
        """
        Executes signals received for Demark indicators

        :param trader: Trader instance
        :param symbols_or_instruments: Symbols to watch
        :param timeframes: Timeframes to watch on
        :param incomplete_bars: Whether or not to process signals for incomplete bars
        :param risk_perc: Account risk percentage for trades
        :param account_denomination: Base denomination of account. used for moving in and out of positions.
        :param margin: Margin allowed?
        :param minimum_margin_level: Maintain this minimum margin level, down to stoploss
        :param trade_signals: Whether or not to live trade signals
        :param price_history_seed: Number of bars to seed the price history with
        :param bootstrap_price_history: Bootstrap the initialization by running the signaler on instantiation
        :param log_level: Optional log level
        """

        self.trader: BinanceTrader = trader
        self.symphony_client: BinanceClient = self.trader.client
        self.account_manager: BinanceAccountManager = self.trader.account_manager
        self.instruments = self.symphony_client.get_all_instruments()
        self.instruments_to_watch = filter_instruments(self.instruments, symbols_or_instruments)
        self.timeframes: Dict[str, List[Timeframe]] = {}
        self.risk_perc: float = risk_perc
        self.trade_margin: bool = margin
        self.minimum_margin_level: float = minimum_margin_level
        self.trade_signals: bool = trade_signals
        self.open_orders: Dict[str, Order] = {}
        self.positions: List[Position] = []

        if account_denomination not in self.symphony_client.get_all_assets():
            raise SignalException(f"Unknown account denomination: {account_denomination}")
        self.account_denomination = account_denomination

        self.symphony_client.register_websocket_callback(self.event_handler)
        self.__incomplete_bars = incomplete_bars
        self.__price_history_seed = price_history_seed
        self.demark_indicators = [
            IndicatorRegistry.BUY_SETUP,
            IndicatorRegistry.SELL_SETUP,
            IndicatorRegistry.PERFECT_BUY_SETUP,
            IndicatorRegistry.PERFECT_SELL_SETUP,
            IndicatorRegistry.BUY_COUNTDOWN,
            IndicatorRegistry.SELL_COUNTDOWN,
            IndicatorRegistry.AGGRESSIVE_BUY_COUNTDOWN,
            IndicatorRegistry.AGGRESSIVE_SELL_COUNTDOWN,
            IndicatorRegistry.BUY_COMBO,
            IndicatorRegistry.SELL_COMBO,
            IndicatorRegistry.BUY_9_13_9,
            IndicatorRegistry.SELL_9_13_9
        ]
        for instrument_or_symbol in symbols_or_instruments:
            for timeframe in self.__timeframes(timeframes):
                self.add_instrument(instrument_or_symbol, timeframe)
                if bootstrap_price_history:
                    instrument = get_instrument(self.instruments, instrument_or_symbol)
                    self.event_handler(self.symphony_client.price_histories[instrument.symbol][timeframe])


        logger.setLevel(log_level)
        return

    def event_handler(self, price_history: PriceHistory) -> None:
        """
        Callback for client

        :param price_history:
        :return:
        """
        if not self.__valid_event(price_history):
            return

        self.apply_indicators(price_history)
        if self.open_orders and price_history.instrument.symbol in self.open_orders.keys():
            if self.open_orders[price_history.instrument.symbol]:
                pass
            return

        indicators_with_signals: List[IndicatorRegistry] = self.find_signals(price_history)
        if indicators_with_signals:
            print(f"{price_history.instrument.symbol} {indicators_with_signals}")
            signals = [self.build_signal(price_history, indicator) for indicator in indicators_with_signals]
            for signal in signals:
                self.execute_signal(signal)

            if self.trade_signals:
                pass
        return

    def build_signal(self, price_history: PriceHistory, indicator: IndicatorRegistry, bar_buffer: Optional[int] = 0) -> Signal:
        """

        :param price_history:
        :return:
        """
        if indicator in [IndicatorRegistry.BUY_SETUP, IndicatorRegistry.BUY_COMBO, IndicatorRegistry.BUY_COUNTDOWN,
                         IndicatorRegistry.BUY_9_13_9, IndicatorRegistry.AGGRESSIVE_BUY_COUNTDOWN]:
            order_side = Market.BUY
        elif indicator in [IndicatorRegistry.SELL_SETUP, IndicatorRegistry.SELL_COMBO, IndicatorRegistry.SELL_COUNTDOWN,
                           IndicatorRegistry.SELL_9_13_9, IndicatorRegistry.AGGRESSIVE_SELL_COUNTDOWN]:
            order_side = Market.SELL
        else:
            raise SignalException(f"Unknown indicator: {indicator}")

        if not price_history.price_history.iloc[-1][indicator.value]:
            raise SignalException(f"Signal not present in price history")

        stop_loss = td_stoploss(price_history, indicator, -1)
        timestamp = price_history.price_history.index[-1]

        signal = Signal()
        signal.instrument = price_history.instrument
        signal.order_side = order_side
        signal.stop_loss = stop_loss
        signal.timeframe = price_history.timeframe
        signal.timestamp = timestamp
        signal.order_id = self.build_order_id(price_history, indicator, order_side, timestamp=timestamp)
        signal.message = f"{indicator.value} on {price_history.instrument} {price_history.timeframe.value} @ {signal.timestamp.to_datetime64()}"
        return signal

    def build_order_id(self,
                       price_history: PriceHistory,
                       indicator: IndicatorRegistry,
                       order_side: Market,
                       timestamp: Optional[pd.Timestamp] = None) -> str:
        """
        Creates the order id for a signal

        :param price_history: Price history object
        :param indicator: Indicator
        :param order_side: Order side
        :param timestamp: Optional timestamp. If not supplied, then use the most recent index of the price history
        :return: Stringified order id
        """
        order_side_short = "b" if order_side == Market.BUY else "s"
        ts = timestamp if timestamp else price_history.price_history.index[-1]
        timestamp_unix = to_unix_time(ts, resolution='s')
        indicator_rep_short = get_string_rep_short(indicator)
        order_id = f"{price_history.instrument.symbol}-{str(price_history.timeframe.value)}-{str(timestamp_unix)}-{order_side_short}-{indicator_rep_short}"
        encoded = base64.b64encode(order_id.encode('ascii')).decode("utf-8")
        encoded = encoded.replace("=", "")
        return encoded

    def find_signals(self, price_history: PriceHistory) -> List[IndicatorRegistry]:
        """
        Returns list of indicators that have signaled, or empty array

        :param price_history: Price history
        :return:
        """
        signals: List[IndicatorRegistry] = []
        df = price_history.price_history
        for indicator in self.demark_indicators:
            if df.iloc[-1][indicator.value] == 1:
                signals.append(indicator)
        return signals


    def execute_signal(self, signal: Signal) -> Position:
        """
        Executes a chosen signal. Can also be registered as a signal handler.

        :param signal: The Signal to execute
        :return: Position entered
        """

        #account_size = self.account_manager.total_free_account_value(self.account_denomination, self.trader.quoter, allowed_spot_assets=self.trader.allowed_spot_assets)
        account_size = 200
        price = self.trader.quoter.get_price(signal.instrument, signal.order_side, fall_back_to_api=True)

        quantity = self.trader.position_sizer.calculate_position_size(
            signal.instrument,
            signal.order_side,
            self.trader.quoter.get_midpoint(signal.instrument, fall_back_to_api=True),
            signal.stop_loss,
            account_size,
            self.account_denomination,
            self.risk_perc,
            margin=self.trade_margin,
            fall_back_to_api=True
        )
        position = None
        breakpoint()
        if self.trade_margin and self.trade_signals and (signal.instrument.isolated_margin_allowed or signal.instrument.margin_allowed):
            margin_deposit_denom, margin_deposit = self.trader.position_sizer.smart_margin(
                signal.instrument,
                price,
                signal.stop_loss,
                quantity,
                signal.order_side,
                signal.instrument.isolated_margin_ratio,
                margin_level=self.minimum_margin_level
            )
            if signal.instrument.isolated_margin_allowed:
                self.account_manager.create_isolated_margin_socket(signal.instrument)
                account_type = AccountType.ISOLATED_MARGIN
            else:
                account_type = AccountType.MARGIN

            position = self.trader.enter_margin_position(
                account_type, signal.instrument, quantity, signal.order_side, margin_deposit, margin_deposit_denom, signal.stop_loss, client_order_id=signal.order_id
            )
            self.positions.append(position)

            logger.info(f"Entered position: {position} in {account_type}")
        elif self.trade_signals:
            quantity = self.trader.round_lot(signal.instrument, quantity)
            pass
        else:
            pass


        breakpoint()
        return Position

    @staticmethod
    def apply_indicators(price_history: PriceHistory) -> PriceHistory:
        """
        Applies all demark indicators

        :param price_history: Price history from event
        :return: None, modifies in place
        """
        bullish_price_flip(price_history)
        bearish_price_flip(price_history)
        td_buy_setup(price_history)
        td_sell_setup(price_history)
        td_buy_countdown(price_history)
        td_sell_countdown(price_history)
        td_buy_combo(price_history)
        td_sell_combo(price_history)
        td_buy_9_13_9(price_history)
        td_sell_9_13_9(price_history)
        td_upwave(price_history)
        td_downwave(price_history)
        return price_history

    def add_instrument(self, instrument_or_symbol: Union[str, Instrument], timeframe: Timeframe) -> None:
        """
        Adds a new instrument to watch for signals

        :param instrument_or_symbol: Instrument to watch
        :param timeframe: Timeframe to watch on
        :return: None
        """
        instrument = get_instrument(self.instruments, instrument_or_symbol)
        self.instruments_to_watch.append(instrument)
        if instrument.symbol not in self.timeframes.keys():
            self.timeframes[instrument.symbol] = [timeframe]
        else:
            self.timeframes[instrument.symbol].append(timeframe)
        # Start candle websocket
        self.symphony_client.start_candle_websocket(instrument.symbol, timeframe, incomplete_bars=self.__incomplete_bars, price_history_seed=self.__price_history_seed)
        # Start isolated margin websocket if trading margin and symbol available
        if self.trade_margin:
            if instrument.isolated_margin_allowed:
                self.account_manager.create_isolated_margin_socket(instrument)
        return

    def __valid_event(self, price_history: PriceHistory) -> bool:
        """
        Filters price history by whether or not this signaler is configured to watch it

        :param price_history: Price history from event
        :return: True or False
        """
        if price_history.instrument.symbol in self.timeframes.keys():
            if price_history.timeframe in self.timeframes[price_history.instrument.symbol]:
                return True
        return False

    def __timeframes(self, timeframes: Union[Timeframe, List[Timeframe]]) -> List[Timeframe]:
        """
        Single or list of timeframes to list

        :param timeframes: Single or list
        :return: list
        """
        if isinstance(timeframes, Timeframe):
            return [timeframes]
        elif isinstance(timeframes, list):
            return [timeframe for timeframe in timeframes if isinstance(timeframe, Timeframe)]
        else:
            raise SignalException(f"Unknown type: {type(timeframes)}")





