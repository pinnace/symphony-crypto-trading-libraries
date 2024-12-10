from jesse.strategies import Strategy, cached
import custom_indicators as cta
from symphony.data_classes import Instrument, PriceHistory
from symphony.enum import Timeframe, string_to_timeframe, Exchange, Column
from symphony.indicator_v2 import IndicatorRegistry
from symphony.indicator_v2.demark.helpers import td_stoploss, is_oversold, is_overbought
from symphony.indicator_v2.demark import td_range_expansion_index, td_demarker_I, td_demarker_II, td_pressure, td_differential, td_anti_differential
from symphony.indicator_v2.candlestick import candlesticks
from symphony.indicator_v2.oscillators import derivative_oscillator, zig_zag, get_harmonics_name
from symphony.indicator_v2.volatility import mass_index, atr
from symphony.indicator_v2.trend import adx
from symphony.utils.time import get_timestamp_of_num_bars_forward
from jesse.utils import risk_to_qty
from symphony.backtest.results import ResultsHelper
from typing import List, Dict, Optional
import pandas_ta as ta
import pandas as pd


class DemarkSellSetup(Strategy):

    def __init__(self):
        super().__init__()
        self.symphony_instrument = None
        self.symphony_timeframe = None
        self.risk_perc = 2
        self.price_history = None
        self.dwave_price_history = None
        self.vars["log_lines"] = []
        self.vars["results"]: List[Dict] = []
        self.vars["curr_max_retracement"] = 0.0
        self.vars["pattern_bars"] = 0
        self.vars["pnl_perc"] = 0.0
        self.results_helper = ResultsHelper("DemarkSellSetup", use_s3=True)
        self.td_setup_max_bars = 300

        self.flags = {
            "TPSupport": True,
            "ExitOnBuySetup": True
        }


    def before(self):
        self.symphony_instrument = self.results_helper.get_instrument(self.symbol.replace("-", ""))
        if isinstance(self.symphony_timeframe, type(None)):
            self.symphony_timeframe = string_to_timeframe(self.timeframe)
        self.price_history = self.td_setup

    @property
    def td_setup(self):
        return cta.td_setup(self.candles, instrument=self.symphony_instrument, timeframe=self.symphony_timeframe, sequential=False, max_bars=self.td_setup_max_bars)

    @property
    def td_dwave(self):
        return cta.td_dwave(self.candles, instrument=self.symphony_instrument, timeframe=self.symphony_timeframe, sequential=True)

    @property
    def timestamp(self) -> str:
        return self.price_history.price_history.iloc[-1][Column.TIMESTAMP]

    def should_long(self) -> bool:
        return False

    def should_short(self) -> bool:
        if self.__sell_setup_present():
            return True
        return False

    def should_cancel(self) -> bool:
        return False

    def go_long(self):
        pass

    def go_short(self):

        if not self.price_history.price_history.iloc[-1][IndicatorRegistry.SELL_SETUP.value]:
            raise Exception("SELL Mismatch")
        self.__place_sell()

    def on_open_position(self, order):
        dwph = self.__update_indicators().price_history

        derivative_oscillator_positive_and_decreasing = False
        if 0.0 < dwph.iloc[-1][IndicatorRegistry.DERIVATIVE_OSCILLATOR.value] < dwph.iloc[-2][IndicatorRegistry.DERIVATIVE_OSCILLATOR.value]:
            derivative_oscillator_positive_and_decreasing = True

        poq_window = 3
        harmonics_window = 3
        harmonics_pattern = "NO_PATTERN"
        for i in range(-1, -harmonics_window - 1, -1):
            if dwph.iloc[i][IndicatorRegistry.HARMONIC.value] != 0:
                harmonics_pattern = get_harmonics_name(dwph.iloc[i][IndicatorRegistry.HARMONIC.value])
        results_obj = {
            "Symbol": self.symphony_instrument.symbol,
            "Timeframe": self.symphony_timeframe.value,
            "Exchange": self.exchange,
            "Direction": "BUY",
            "EntryTimestamp": str(self.timestamp),
            "IsPerfect": True if self.price_history.price_history.iloc[-1][IndicatorRegistry.PERFECT_BUY_SETUP.value] else False,
            IndicatorRegistry.DWAVE_UP.value: dwph.iloc[-1][IndicatorRegistry.DWAVE_UP.value],
            IndicatorRegistry.DWAVE_DOWN.value: dwph.iloc[-1][IndicatorRegistry.DWAVE_DOWN.value],
            IndicatorRegistry.DERIVATIVE_OSCILLATOR.value: dwph.iloc[-1][IndicatorRegistry.DERIVATIVE_OSCILLATOR.value],
            IndicatorRegistry.DERIVATIVE_OSCILLATOR_SIGNAL.value: dwph.iloc[-1][IndicatorRegistry.DERIVATIVE_OSCILLATOR_SIGNAL.value],
            "DerivativeOscillatorRule": derivative_oscillator_positive_and_decreasing,
            IndicatorRegistry.CANDLESTICK_PATTERN.value: dwph.iloc[-1][IndicatorRegistry.CANDLESTICK_PATTERN.value],
            IndicatorRegistry.CANDLESTICK_PATTERN_DIRECTION.value: dwph.iloc[-1][IndicatorRegistry.CANDLESTICK_PATTERN_DIRECTION.value],
            IndicatorRegistry.RSI.value: dwph.iloc[-1][IndicatorRegistry.RSI.value],
            IndicatorRegistry.MASS_INDEX.value: dwph.iloc[-1][IndicatorRegistry.MASS_INDEX.value],
            IndicatorRegistry.NATR.value: dwph.iloc[-1][IndicatorRegistry.NATR.value],
            IndicatorRegistry.ADX.value: dwph.iloc[-1][IndicatorRegistry.ADX.value],
            IndicatorRegistry.TD_RANGE_EXPANSION_INDEX.value: dwph.iloc[-1][IndicatorRegistry.TD_RANGE_EXPANSION_INDEX.value],
            IndicatorRegistry.TD_POQ.value: any(dwph[IndicatorRegistry.TD_POQ.value].iloc[-poq_window:] == ["BUY"] * poq_window),
            IndicatorRegistry.TD_DEMARKER_I.value: dwph[IndicatorRegistry.TD_DEMARKER_I.value].iloc[-1],
            "DemarkerIOversold": is_oversold(self.dwave_price_history, IndicatorRegistry.TD_DEMARKER_I),
            "DemarkerIOverbought": is_overbought(self.dwave_price_history, IndicatorRegistry.TD_DEMARKER_I),
            IndicatorRegistry.TD_PRESSURE.value: dwph[IndicatorRegistry.TD_PRESSURE.value].iloc[-1],
            "TDPressureOversold": is_oversold(self.dwave_price_history, IndicatorRegistry.TD_PRESSURE),
            "TDPressureOverbought": is_overbought(self.dwave_price_history, IndicatorRegistry.TD_PRESSURE),
            IndicatorRegistry.ZIGZAG.value: True if dwph.iloc[-1][IndicatorRegistry.ZIGZAG.value] != 0 else False,
            IndicatorRegistry.HARMONIC.value: harmonics_pattern,
            IndicatorRegistry.TD_DIFFERENTIAL.value: dwph.iloc[-1][IndicatorRegistry.TD_DIFFERENTIAL.value],
            IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value: dwph.iloc[-1][IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value]
        }
        self.vars["results"][-1] = {**self.vars["results"][-1], **results_obj}
        self.vars["curr_max_retracement"] = order.price / order.price
        return

    def on_stop_loss(self, order):

        self.__log_line(f"Hit stop loss for {self.position.id} at {self.timestamp}")
        self.__log_exit_result(False, append_to_result=True, one_bar_close=self.__is_one_bar_close())
        self.__clear_vars()
        return

    def __is_one_bar_close(self) -> bool:
        if self.vars["results"][-1]["EntryTimestamp"] == str(self.timestamp):
            if "ExitTimestamp" in self.vars["results"][-1].keys():
                breakpoint()
                raise Exception("Unexpected")
            return True
        return False

    def on_take_profit(self, order):

        self.__log_line(f"Hit take profit for {self.position.id} at {self.timestamp}")
        self.__log_exit_result(True, append_to_result=True, one_bar_close=self.__is_one_bar_close())
        self.__clear_vars()
        return

    def update_position(self):
        self.__update_vars()

        if self.flags["ExitOnBuySetup"] and self.__buy_setup_present(index=-1):
            self.__exit_pos(exit_condition="ExitOnBuySetup")

        return

    def terminate(self):
        df = self.price_history.price_history
        rdf = self.results_helper.results_df

        for line in self.vars["log_lines"]:
            print(line)
        self.results_helper.flush()

    def __update_indicators(self) -> PriceHistory:
        self.dwave_price_history = self.td_dwave
        candlesticks(self.dwave_price_history)
        derivative_oscillator(self.dwave_price_history)
        mass_index(self.dwave_price_history)
        atr(self.dwave_price_history, normalized=True)
        adx(self.dwave_price_history)
        td_range_expansion_index(self.dwave_price_history)
        td_demarker_I(self.dwave_price_history)
        td_pressure(self.dwave_price_history)
        zig_zag(self.dwave_price_history)
        td_differential(self.dwave_price_history)
        td_anti_differential(self.dwave_price_history)
        return self.dwave_price_history

    def __place_sell(self, index=-1):
        stop_loss = td_stoploss(self.price_history, IndicatorRegistry.SELL_SETUP, index)

        qty = risk_to_qty(self.capital, self.risk_perc, self.close, stop_loss, fee_rate=0.001)
        price = self.close if index == -1 else self.open
        self.sell = qty, price
        self.stop_loss = qty, stop_loss

        if self.flags["TPSupport"]:
            take_profit_level = self.price_history.price_history.iloc[index][IndicatorRegistry.TDST_SUPPORT.value]
            self.take_profit = qty, take_profit_level
            profit_factor = ((price - take_profit_level) / price) / (self.risk_perc / 100)
            results_obj = {
                "Price": price,
                "StopLoss": stop_loss,
                "TakeProfit": take_profit_level,
                "ProfitFactor": profit_factor
            }
        else:
            results_obj = {
                "Price": price,
                "StopLoss": stop_loss
            }

        self.vars["results"].append(results_obj) # Creation
        self.__log_line(f"Placed SELL {self.position.id} for {self.symbol} with SL: {stop_loss}, Qty: {qty} @ {self.timestamp}")

        return

    def __exit_pos(self, exit_condition: str = "N/A") -> None:
        self.__log_line(f"Liquidating short {{{self.position.id}}} at {self.timestamp} with PNL: "
                        f"{self.position.pnl_percentage} @ {self.timestamp}. Reason: {exit_condition}")
        self.__log_exit_result(True, append_to_result=True)
        self.liquidate()
        self.__clear_vars()
        return

    def __buy_setup_present(self, index=-1) -> bool:
        if self.price_history.price_history.iloc[index][IndicatorRegistry.BUY_SETUP.value]:
            return True
        return False

    def __sell_setup_present(self, index=-1) -> bool:
        if self.price_history.price_history.iloc[index][IndicatorRegistry.SELL_SETUP.value]:
            return True
        return False

    def __log_line(self, log_line: str):
        self.vars["log_lines"].append(log_line)

    def __update_vars(self):
        self.vars["pnl_perc"] = self.position.pnl_percentage

        self.vars["pattern_bars"] += 1
        retracement = self.high / self.position.entry_price
        if retracement > self.vars["curr_max_retracement"]:
            self.vars["curr_max_retracement"] = retracement
        return

    def __clear_vars(self):
        self.vars["curr_max_retracement"] = 0.0
        self.vars["pattern_bars"] = 0
        self.vars["pnl_perc"] = 0.0

    def __log_exit_result(self, profitable: bool, append_to_result: Optional[bool] = False, one_bar_close: Optional[bool] = False) -> Dict:
        # Handle edge condition
        if one_bar_close:
            tp_level = self.vars["results"][-1]["TakeProfit"]
            stop_loss = self.vars["results"][-1]["StopLoss"]
            entry_price = self.vars["results"][-1]["Price"]
            if profitable:
                pnl_perc = ((tp_level - entry_price) / entry_price) * 100
            else:
                pnl_perc = ((stop_loss - entry_price) / entry_price) * 100
            ts = pd.Timestamp(self.vars["results"][-1]["EntryTimestamp"], tz='utc')
            exit_timestamp = get_timestamp_of_num_bars_forward(self.symphony_timeframe, ts, 1).strftime('%Y-%m-%d %H:%M:%S')
            bars = 1
            if profitable:
                retracement = tp_level / entry_price
            else:
                retracement = 0.0
        else:
            pnl_perc = self.vars["pnl_perc"]
            exit_timestamp = str(self.timestamp)
            bars = self.vars["pattern_bars"]
            retracement = self.vars["curr_max_retracement"]

        results_obj = {
            "Profitable": profitable,
            "PNLPerc": pnl_perc,
            "MaxRetracement": retracement,
            "BarsMaxRetracement": bars,
            "ExitTimestamp": exit_timestamp
        }


        if append_to_result:
            # Careful not to duplicate
            dupe = False
            for result in self.vars["results"]:
                if "ExitTimestamp" in result.keys() and str(self.timestamp) == result["ExitTimestamp"]:
                    dupe = True
            if not dupe:
                # Another edge case for same bar closes
                if len(self.vars["results"]) > 1 and self.vars["results"][-1]["EntryTimestamp"] == self.vars["results"][-2]["EntryTimestamp"]:
                    pass
                else:
                    self.vars["results"][-1] = {**self.vars["results"][-1], **results_obj}
                    self.results_helper.append_result(self.vars["results"][-1])
        return results_obj
