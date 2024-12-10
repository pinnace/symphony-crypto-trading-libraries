import numpy as np
import talib
from typing import Union
from symphony.enum import Column, Timeframe
from symphony.data_classes import PriceHistory, Instrument
from symphony.indicator_v2.demark import td_upwave, td_downwave, td_buy_setup, td_sell_setup, td_buy_countdown, td_sell_countdown, td_buy_9_13_9, td_sell_9_13_9, \
    bullish_price_flip, bearish_price_flip, td_buy_combo, td_sell_combo
from jesse.helpers import get_candle_source, slice_candles
from jesse.utils import numpy_candles_to_dataframe
from typing import Optional
from logging import ERROR


def td_setup(candles: np.ndarray, instrument: Instrument, timeframe: Timeframe, sequential: Optional[bool] = False, max_bars: Optional[int] = -1) -> PriceHistory:
    candles = slice_candles(candles, sequential)
    df = numpy_candles_to_dataframe(candles, name_date=Column.TIMESTAMP, name_open=Column.OPEN, name_high=Column.HIGH, name_low=Column.LOW, name_volume=Column.VOLUME)
    price_history = PriceHistory()
    price_history.price_history = df
    price_history.instrument = instrument
    price_history.timeframe = timeframe

    bullish_price_flip(price_history)
    bearish_price_flip(price_history)
    td_buy_setup(price_history, max_bars=max_bars)
    td_sell_setup(price_history, max_bars=max_bars)
    #td_buy_countdown(price_history, log_level=ERROR)
    #td_sell_countdown(price_history, log_level=ERROR)
    #td_buy_combo(price_history, log_level=ERROR)
    #td_sell_combo(price_history, log_level=ERROR)
    #td_buy_9_13_9(price_history, log_level=ERROR)
    #td_sell_9_13_9(price_history, log_level=ERROR)
    #td_upwave(price_history, log_level=ERROR)
    #td_downwave(price_history, log_level=ERROR)

    return price_history

def td_countdown(candles: np.ndarray, instrument: Instrument, timeframe: Timeframe, sequential: Optional[bool] = False, max_bars: Optional[int] = -1) -> PriceHistory:
    candles = slice_candles(candles, sequential)
    df = numpy_candles_to_dataframe(candles, name_date=Column.TIMESTAMP, name_open=Column.OPEN, name_high=Column.HIGH, name_low=Column.LOW, name_volume=Column.VOLUME)
    price_history = PriceHistory()
    price_history.price_history = df
    price_history.instrument = instrument
    price_history.timeframe = timeframe

    bullish_price_flip(price_history)
    bearish_price_flip(price_history)
    td_buy_setup(price_history, max_bars=max_bars)
    td_sell_setup(price_history, max_bars=max_bars)
    td_buy_countdown(price_history, log_level=ERROR)
    td_sell_countdown(price_history, log_level=ERROR)
    #td_buy_combo(price_history, log_level=ERROR)
    #td_sell_combo(price_history, log_level=ERROR)
    #td_buy_9_13_9(price_history, log_level=ERROR)
    #td_sell_9_13_9(price_history, log_level=ERROR)
    #td_upwave(price_history, log_level=ERROR)
    #td_downwave(price_history, log_level=ERROR)

    return price_history

def td_dwave(candles: np.ndarray, instrument: Instrument, timeframe: Timeframe, sequential: Optional[bool] = True) -> PriceHistory:
    candles = slice_candles(candles, sequential)
    df = numpy_candles_to_dataframe(candles, name_date=Column.TIMESTAMP, name_open=Column.OPEN, name_high=Column.HIGH, name_low=Column.LOW, name_volume=Column.VOLUME)
    price_history = PriceHistory()
    price_history.price_history = df
    price_history.instrument = instrument
    price_history.timeframe = timeframe
    td_upwave(price_history, log_level=ERROR)
    td_downwave(price_history, log_level=ERROR)

    return price_history