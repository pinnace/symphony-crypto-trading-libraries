from symphony.data_classes import PriceHistory
from ..indicator_registry import IndicatorRegistry
from typing import Optional
import numpy as np
import pandas_ta as ta


def derivative_oscillator(price_history: PriceHistory,
                          rsi_len: Optional[int] = 14,
                          first_ema: Optional[int] = 5,
                          second_ema: Optional[int] = 3,
                          sma_length: Optional[int] = 9,
                          signal_length: Optional[int] = 9) -> PriceHistory:
    """
    Calculates the Derivative Oscillator. Also sets RSI.

    https://www.tradingview.com/script/9Wp8VPYf-Derivative-Oscillator-Cu-ID-AC-P/

    :param price_history: Standard price history
    :param rsi_len: For RSI
    :param first_ema: First RSI smoothing length
    :param second_ema: Second RSI smoothing length
    :param sma_length: Third RSI smoothing length
    :param signal_length: Signal line length
    :return: PriceHistory with indicator applied
    """
    df = price_history.price_history
    df['rsi'] = df.ta.rsi(length=rsi_len)
    smoothed_rsi = ta.ema(df['rsi'], length=first_ema)
    smoothed_rsi = ta.ema(smoothed_rsi, length=second_ema)
    dosc = smoothed_rsi - ta.sma(smoothed_rsi, sma_length)
    signal = ta.sma(dosc, signal_length)
    df[IndicatorRegistry.DERIVATIVE_OSCILLATOR.value] = dosc
    df[IndicatorRegistry.DERIVATIVE_OSCILLATOR_SIGNAL.value] = signal
    return price_history