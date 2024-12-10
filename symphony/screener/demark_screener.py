from typing import Callable, NewType, List, Union
from symphony.enum import Exchange, Timeframe
from symphony.config import LOG_LEVEL, USE_MODIN
from symphony.client import exchange_client
from symphony.data_classes import PriceHistory, Instrument, filter_instruments
from symphony.indicator_v2 import IndicatorRegistry
from symphony.abc import ClientABC
from symphony.indicator_v2.demark import bullish_price_flip, bearish_price_flip, td_buy_setup, td_sell_setup, \
    td_buy_countdown, td_sell_countdown, td_buy_combo, td_sell_combo, td_buy_9_13_9, td_sell_9_13_9, \
    td_upwave, td_downwave
from symphony.indicator_v2.demark.helpers import td_stoploss
import logging
from time import perf_counter
from symphony.utils.time import get_timestamp_of_num_bars_back, filter_start
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

ExchangeType = NewType('ExchangeType', Exchange)
IndicatorType = NewType('Indicator', IndicatorRegistry)
logger = logging.getLogger(__name__)


class DemarkScreener:
    """

    """
    def __init__(self,
                 timeframes: Union[Timeframe, List[Timeframe]],
                 symbols_or_instruments: Union[List[str], List[Instrument]] = None,
                 log_level: int = LOG_LEVEL
                 ):
        self.price_histories: List[PriceHistory] = []
        self.timeframes = timeframes
        self.symbols_or_instruments: Union[List[str], List[Instrument]] = symbols_or_instruments
        self.messages: dict[Instrument] = {}
        logger.setLevel(log_level)

    def fetch(self, exchange: ExchangeType, num_bars: int, ):
        """

        :param exchange:
        :param num_bars:
        :return:
        """
        ex_client: ClientABC = exchange_client(exchange)()
        instruments: List[Instrument] = filter_instruments(
            ex_client.get_all_symbols(),
            self.symbols_or_instruments
        )

        self.price_histories: List[PriceHistory] = ex_client.get_multiple(instruments, self.timeframes, num_bars)

    def process(self):
        start_process_time: float = perf_counter()
        timings: List[float] = []
        price_history: PriceHistory
        for price_history in self.price_histories:
            start_time: float = perf_counter()

            bullish_price_flip(price_history)
            bearish_price_flip(price_history)
            td_buy_setup(price_history)
            td_sell_setup(price_history)
            td_buy_countdown(price_history)
            td_sell_countdown(price_history)
            td_buy_9_13_9(price_history)
            td_sell_9_13_9(price_history)
            td_buy_combo(price_history)
            td_sell_combo(price_history)
            td_upwave(price_history)
            td_downwave(price_history)

            end_time: float = perf_counter()
            timings.append(end_time - start_time)

        average_time: float = sum(timings) / len(timings)
        logger.debug("Average Execution time: {:10.4f}s".format(average_time))
        end_process_time: float = perf_counter()
        logger.debug("Total Execution time: {:10.4f}s".format(end_process_time - start_process_time))
        return

    def filter(self, bar_threshold: int):

        def set_key(instrument: Instrument):
            if instrument not in self.messages.keys():
                self.messages[instrument] = {}

        def build_message(price_history: PriceHistory, index: pd.Timestamp, indicator: IndicatorType) -> dict:
            return {
                "pattern": indicator.value.upper(),
                "exchange": price_history.instrument.exchange.value.upper(),
                "datetime": str(index),
                "stop_loss": td_stoploss(price_history, indicator, index)
            }

        def create_messages(price_history: PriceHistory) -> dict:
            df = price_history.price_history
            start_ts: pd.Timestamp = get_timestamp_of_num_bars_back(price_history.timeframe, df.index[-1], bar_threshold)

            buy_countdowns: List[pd.Timestamp] = filter_start(
                df.index[df[IndicatorRegistry.BUY_COUNTDOWN.value] == 1].tolist(), start_ts)
            sell_countdowns: List[pd.Timestamp] = filter_start(
                df.index[df[IndicatorRegistry.SELL_COUNTDOWN.value] == 1].tolist(), start_ts)
            buy_combos: List[pd.Timestamp] = filter_start(
                df.index[df[IndicatorRegistry.BUY_COMBO.value] == 1].tolist(), start_ts)
            sell_combos: List[pd.Timestamp] = filter_start(
                df.index[df[IndicatorRegistry.SELL_COMBO.value] == 1].tolist(), start_ts)
            buy_9_13_9s: List[pd.Timestamp] = filter_start(
                df.index[df[IndicatorRegistry.BUY_9_13_9.value] == 1].tolist(), start_ts)
            sell_9_13_9s: List[pd.Timestamp] = filter_start(
                df.index[df[IndicatorRegistry.SELL_9_13_9.value] == 1].tolist(), start_ts)

            if buy_countdowns:
                assert len(buy_countdowns) == 1
                set_key(price_history.instrument)
                self.messages[price_history.instrument][price_history.timeframe] = \
                    build_message(price_history, buy_countdowns[0], IndicatorRegistry.BUY_COUNTDOWN)
            if sell_countdowns:
                assert len(sell_countdowns) == 1
                set_key(price_history.instrument)
                self.messages[price_history.instrument][price_history.timeframe] = \
                    build_message(price_history, sell_countdowns[0], IndicatorRegistry.SELL_COUNTDOWN)
            if buy_combos:
                assert len(buy_combos) == 1
                set_key(price_history.instrument)
                self.messages[price_history.instrument][price_history.timeframe] = \
                    build_message(price_history, buy_combos[0], IndicatorRegistry.BUY_COMBO)
            if sell_combos:
                assert len(sell_combos) == 1
                set_key(price_history.instrument)
                self.messages[price_history.instrument][price_history.timeframe] = \
                    build_message(price_history, sell_combos[0], IndicatorRegistry.SELL_COMBO)
            if buy_9_13_9s:
                assert len(buy_9_13_9s) == 1
                set_key(price_history.instrument)
                self.messages[price_history.instrument][price_history.timeframe] = \
                    build_message(price_history, buy_9_13_9s[0], IndicatorRegistry.BUY_9_13_9)
            if sell_9_13_9s:
                assert len(sell_9_13_9s) == 1
                set_key(price_history.instrument)
                self.messages[price_history.instrument][price_history.timeframe] = \
                    build_message(price_history, sell_9_13_9s[0], IndicatorRegistry.SELL_9_13_9)
            return

        start_message_build_time: float = perf_counter()
        list(map(create_messages, self.price_histories))
        end_message_build_time: float = perf_counter()
        logger.debug("Total Message Build time: {:10.4f}s".format(end_message_build_time - start_message_build_time))
        breakpoint()

