import backtrader as bt
from symphony.backtest.indicators import DemarkCountdownBtIndicatorWrapper
from symphony.indicators.demarkcountdown.td_utils import get_td_countdown_pattern_start, get_td_countdown_stoploss, \
    get_td_pattern_ohclv
from symphony.indicators import IndicatorKit, IndicatorRegistry
from symphony.schema import SchemaKit
from symphony.risk_management.historical_rate_converter import HistoricalRatesConverter
import pandas as pd
from datetime import datetime
import json
import uuid
import os
import pathlib
import glob


class DemarkCountdownStrategy(bt.Strategy):
    demark_line_names = (
    "bearish_price_flips", "buy_setups", "buy_setups_true_end", "perfect_buy_setups", "tdst_resistance", \
    "active_buy_setups", "buy_countdowns", "bullish_price_flips", "sell_setups", "sell_setups_true_end",
    "perfect_sell_setups", \
    "tdst_support", "active_sell_setups", "sell_countdowns", "tradeable_buy_setups", "tradeable_sell_setups",)

    def __init__(self, outputdir, instrument, timeframe, digits, position_size, risk_perc=0.02,
                 # Optimization options
                 cancellation_qualifier_I=False,  #
                 cancellation_qualifier_II=False,  #
                 adx_simple=False,  #
                 adx_lookback=False,  #
                 rsi_range=False,  #
                 atr_stop=False,
                 trailing_stop=False,  #
                 fib_take_profit_level=1.0,  #
                 close_on_conflict=False

                 ):

        self.flow = SchemaKit.standard_flow()
        self.broker.set_coc(True)

        self.digits = digits
        self.dt_format = "%Y-%m-%d %H:%M:%S"

        # Init optimization params
        self.cancellation_qualifier_I = cancellation_qualifier_I
        self.cancellation_qualifier_II = cancellation_qualifier_II
        self.adx_simple = adx_simple
        self.adx_lookback = adx_lookback
        self.rsi_range = rsi_range
        self.trailing_stop = trailing_stop
        self.atr_stop = atr_stop
        self.fib_take_profit_level = fib_take_profit_level
        self.close_on_conflict = close_on_conflict
        self.coc_flag = False

        # Init indicators
        self.demark = DemarkCountdownBtIndicatorWrapper(cancellation_qualifier_I=self.cancellation_qualifier_I,
                                                        cancellation_qualifier_II=self.cancellation_qualifier_II)
        self.rsi = bt.talib.RSI()
        self.adx = bt.indicators.DirectionalMovementIndex()
        self.atr = bt.indicators.ATR()
        self.ichimoku = bt.indicators.Ichimoku()

        # Init config
        self.outputdir = outputdir
        self.position_size = position_size

        if self.outputdir is not None:
            path = pathlib.Path(self.outputdir)
            path.mkdir(parents=True, exist_ok=True)

        self.instrument = instrument
        self.timeframe = timeframe
        self.risk_perc = risk_perc
        self.hrc = HistoricalRatesConverter('Oanda', self.timeframe, self.instrument, 10000)
        # Backtrader order obj
        self.order = None
        # Order dataframe

        self.order_df = None
        self.order_params = {
            "open_price": None,
            "stop_loss": None,
            "take_profit": None,
            "profit_factor": None,
            "type": None,  # "BUY" or "SELL"
            "time": None,
            "posix_time": None
        }

        self.trade_count = 0
        self.sell_trade_count = 0
        self.buy_trade_count = 0
        self.total_profit_in_pips = 0
        self.total_profit = 0
        self.true_return = 0

    def stop(self):
        print("[{}] Backtesting ending. Total trades: {}".format(self.instrument, self.sell_trade_count + self.buy_trade_count))
        print("[{}|{}] P/L: {}".format(self.instrument, self.timeframe, round(self.total_profit, 2)))

    def notify_trade(self, trade):
        if trade.isopen:
            pass
        elif trade.isclosed:
            self.total_profit += trade.pnl
            print('Closing trade, gross %.2f, net %.2f' % (trade.pnl, trade.pnlcomm))
            breakpoint()

    def notify_order(self, order):
        if self.trailing_stop:
            breakpoint()
        if not order.status == order.Completed:
            breakpoint()
            return  # discard any other notification
        executed_price = round(order.executed.price, self.digits)
        # Order exit logic
        if not self.position:  # we left the market
            profitable = False
            profit_in_pips = 0
            if self.order_params["type"] == "BUY":

                profit_in_pips = int((executed_price - self.order_params["open_price"]) * (10 ** (self.digits)))

                if executed_price == self.order_params["stop_loss"]:
                    print('[EXIT_BUY_SL] STOP_LOSS@price: {:.5f} -- PiP: {}'.format(executed_price, profit_in_pips))
                elif executed_price <= self.order_params["stop_loss"]:
                    print(
                        '[EXIT_BUY_SL_SLIP] STOP_LOSS@price: {:.5f} -- PiP: {}'.format(executed_price, profit_in_pips))
                elif executed_price == self.order_params["take_profit"]:
                    print('[EXIT_BUY_TP] TAKE_PROFIT@price: {:.5f} -- PiP: {}'.format(executed_price, profit_in_pips))
                    profitable = True
                elif executed_price >= self.order_params["take_profit"]:
                    print('[EXIT_BUY_TP_SLIP] TAKE_PROFIT@price: {:.5f} -- PiP: {}'.format(executed_price,
                                                                                           profit_in_pips))
                    profitable = True
                elif self.close_on_conflict and self.coc_flag:

                    if executed_price > self.order_params["open_price"]:
                        print('[EXIT_BUY_CONFLICT_TP] TAKE_PROFIT@price: {:.5f} -- PiP: {}'.format(executed_price,
                                                                                                   profit_in_pips))
                        profitable = True
                    elif executed_price <= self.order_params["open_price"]:
                        print('[EXIT_BUY_CONFLICT_SL] STOP_LOSS@price: {:.5f} -- PiP: {}'.format(executed_price,
                                                                                                 profit_in_pips))
                    else:
                        raise Exception("Error for COC BUY")
                else:
                    if self.trailing_stop:
                        print('[EXIT_BUY_SL_TRAIL] STOP_LOSS@price: {:.5f} -- PiP: {}'.format(executed_price,
                                                                                              profit_in_pips))
                    else:
                        print(self.order_params)
                        raise Exception("Error for BUY, closing price equals neither stoploss nor takeprofit: \
                                    \n\tClose: {}\n\tSL: {}\n\tTP: {}".format(executed_price,
                                                                              self.order_params["stop_loss"],
                                                                              self.order_params["take_profit"]))

            if self.order_params["type"] == "SELL":

                profit_in_pips = int((self.order_params["open_price"] - executed_price) * (10 ** self.digits))
                if executed_price == self.order_params["stop_loss"]:
                    print('[EXIT_SELL_SL] STOP_LOSS@price: {:.5f} -- PiP: {}'.format(executed_price, profit_in_pips))
                elif executed_price >= self.order_params["stop_loss"]:
                    print(
                        '[EXIT_SELL_SL_SLIP] STOP_LOSS@price: {:.5f} -- PiP: {}'.format(executed_price, profit_in_pips))
                elif executed_price == self.order_params["take_profit"]:
                    print('[EXIT_SELL_TP] TAKE_PROFIT@price: {:.5f} -- PiP: {}'.format(executed_price, profit_in_pips))
                    profitable = True
                elif executed_price <= self.order_params["take_profit"]:
                    print('[EXIT_SELL_TP_SLIP] TAKE_PROFIT@price: {:.5f} -- PiP: {}'.format(executed_price,
                                                                                            profit_in_pips))
                    profitable = True
                elif self.close_on_conflict and self.coc_flag:

                    if executed_price < self.order_params["open_price"]:
                        print('[EXIT_SELL_CONFLICT_TP] TAKE_PROFIT@price: {:.5f} -- PiP: {}'.format(executed_price,
                                                                                                    profit_in_pips))
                        profitable = True
                    elif executed_price >= self.order_params["open_price"]:
                        print('[EXIT_SELL_CONFLICT_SL] STOP_LOSS@price: {:.5f} -- PiP: {}'.format(executed_price,
                                                                                                  profit_in_pips))
                    else:
                        raise Exception("Error for COC SELL")

                else:
                    if self.trailing_stop:
                        print('[EXIT_SELL_SL_TRAIL] STOP_LOSS@price: {:.5f} -- PiP: {}'.format(executed_price,
                                                                                               profit_in_pips))
                    else:
                        print(self.order_params)
                        raise Exception("Error for SELL, closing price equals neither stoploss nor takeprofit: \
                                    \n\tClose: {}\n\tSL: {}\n\tTP: {}".format(order.executed.price,
                                                                              self.order_params["stop_loss"],
                                                                              self.order_params["take_profit"]))

            # Dump metadata to timestamp.label file, dataframe to timestamp.csv

            metadata = {
                "profitable": 1 if profitable else 0,
                "profit_in_pips": profit_in_pips,
                "profit_factor": self.order_params["profit_factor"],
                "instrument": self.instrument,
                "order_type": self.order_params["type"],
                "time": self.order_params["time"],
                "posix_time": self.order_params["posix_time"]
            }
            self.total_profit_in_pips += profit_in_pips
            self.write_pattern(self.order_df, metadata)
            self.reset_order_data()

        # Order enter logic
        else:
            try:
                if order.isbuy():
                    if self.order_params["open_price"] != executed_price:
                        dt = self.data.datetime.datetime().strftime(self.dt_format)
                        breakpoint()
                        print("[!] For BUY, entered at a different price than last close - Open[{}] Executed[{}] Datetime[{}]".format(self.order_params["open_price"], executed_price, dt))
                    self.order_params["open_price"] = executed_price
                    print('[ENTER] BUY @price: {:.5f}, Profit Factor: {}'.format(executed_price,
                                                                                 self.order_params["profit_factor"]))
                    self.buy_trade_count += 1

                if order.issell():
                    if self.order_params["open_price"] != executed_price:
                        dt = self.data.datetime.datetime().strftime(self.dt_format)
                        breakpoint()
                        print("[!] For SELL, entered at a different price than last close - Open[{}] Executed[{}] Datetime[{}]".format(self.order_params["open_price"], executed_price, dt))
                    self.order_params["open_price"] = executed_price
                    print('[ENTER] SELL @price: {:.5f}, Profit Factor: {}'.format(executed_price,
                                                                                  self.order_params["profit_factor"]))
                    self.sell_trade_count += 1
            except Exception as e:
                print(e)
                breakpoint()

        return

    def next(self):
        # print(len(self.data.close))
        if (self.demark.lines.buy_countdowns[0] or self.demark.lines.sell_countdowns[0]) and self.order_df is None:
            # breakpoint()
            length = len(self.data.close) - 1
            opens = list(self.data.open.get(ago=-1, size=length)) + [self.data.open[0]]
            highs = list(self.data.high.get(ago=-1, size=length)) + [self.data.high[0]]
            lows = list(self.data.low.get(ago=-1, size=length)) + [self.data.low[0]]
            closes = list(self.data.close.get(ago=-1, size=length)) + [self.data.close[0]]
            volumes = list(self.data.volume.get(ago=-1, size=length)) + [self.data.volume[0]]

            self.flow["price_history"] = [[o, h, l, c, v] for o, h, l, c, v in zip(opens, highs, lows, closes, volumes)]

            # Create temp object with all demark channels
            demark_channels = {}
            for line_name in self.demark_line_names:
                line = getattr(self.demark.lines, line_name)
                demark_channels[line_name] = list(line.get(ago=-1, size=length)) + [line[0]]

            # Either add or update all channels in flow obj
            ind = IndicatorKit.get_indicator_for_flow(IndicatorRegistry.TD_COUNTDOWN.name, {}, demark_channels)

            if IndicatorKit.indicator_present_in_flow(self.flow, IndicatorRegistry.TD_COUNTDOWN.name.lower()):
                IndicatorKit.update_indicator_for_flow(self.flow, ind)
            else:
                IndicatorKit.insert_indicator_into_flow(self.flow, ind)

            pattern_start = get_td_countdown_pattern_start(self.flow, length)

            # Get trading parameters
            stop_loss = get_td_countdown_stoploss(self.flow, length)

            # If buy countdown
            if self.demark.lines.buy_countdowns[0]:

                # IMPL FIB_TP
                price_tp_diff = abs(closes[-1] - self.demark.lines.tdst_resistance[0]) * self.fib_take_profit_level
                take_profit = round(closes[-1] + price_tp_diff, self.digits)

                # IMPL 1ATR STOP
                if self.atr_stop:
                    stop_loss = round(stop_loss - self.atr[0], self.digits)
            # If buy countdown
            else:
                # IMPL FIB_TP
                price_tp_diff = abs(closes[-1] - self.demark.lines.tdst_support[0]) * self.fib_take_profit_level
                take_profit = round(closes[-1] - price_tp_diff, self.digits)

                # IMPL 1ATR STOP
                if self.atr_stop:
                    stop_loss = round(stop_loss + self.atr[0], self.digits)

            # take_profit = self.demark.lines.tdst_resistance[0] if self.demark.lines.buy_countdowns[0] else self.demark.lines.tdst_support[0]

            stop_in_pips = int(abs((closes[-1] - stop_loss) * (10 ** self.digits - 1)))
            units = self.hrc.get_units(self.risk_perc, self.broker.get_cash(), stop_in_pips, self.data.datetime.date().strftime('%Y-%m-%d %H:%M:%S'))
            units *= 10
            print("UNITS: {}".format(units))
            # IMPL TRAILING_STOP
            if self.trailing_stop:
                #args = {"limitprice": take_profit, "trailamount": round(abs(closes[-1] - stop_loss), self.digits),
                        #"exectype": bt.Order.StopTrail, "size": units}
                args = {"trailpercent": 0.02, "exectype": bt.Order.StopTrail}
                breakpoint()
            else:
                args = {"limitprice": take_profit, "stopprice": stop_loss, "exectype": bt.Order.Market, "size": units}

            buy_or_sell: str = "BUY" if self.demark.lines.buy_countdowns[0] else "SELL"

            # Collect metadata
            print("{} countdown found, open, high, low, close, datetime: {} {} {} {} {}".format(
                buy_or_sell,
                self.data.open[0],
                self.data.high[0],
                self.data.low[0],
                self.data.close[0],
                self.datetime[0]
            ))
            print("Take Profit: {}, Stop Loss: {}".format(take_profit, stop_loss))
            self.order_params["open_price"] = round(closes[-1], self.digits)
            self.order_params["stop_loss"] = round(stop_loss, self.digits)
            self.order_params["take_profit"] = round(take_profit, self.digits)
            self.order_params["profit_factor"] = abs(
                self.order_params["open_price"] - self.order_params["take_profit"]) / abs(
                self.order_params["open_price"] - self.order_params["stop_loss"])
            self.order_params["type"] = buy_or_sell
            timestr = str(self.data.datetime.date()) + ' ' + str(self.data.datetime.time())
            ts = pd.Timestamp(timestr).round(freq='s')

            self.order_params["time"] = ts.strftime('%Y-%m-%d %H:%M:%S')
            self.order_params["posix_time"] = int(ts.timestamp())

            pattern = get_td_pattern_ohclv(self.flow, pattern_start)
            self.order_df = pd.DataFrame(data=pattern)

            # Get some additional indicators
            self.order_df["RSI"] = list(self.rsi.get(ago=-1, size=length - pattern_start)) + [self.rsi[0]]
            self.order_df["ADX"] = list(self.adx.get(ago=-1, size=length - pattern_start)) + [self.adx[0]]
            self.order_df["PLUS_DI"] = list(self.adx.DIminus.get(ago=-1, size=length - pattern_start)) + [
                self.adx.DIminus[0]]
            self.order_df["MINUS_DI"] = list(self.adx.DIplus.get(ago=-1, size=length - pattern_start)) + [
                self.adx.DIplus[0]]
            self.order_df["ATR"] = list(self.atr.get(ago=-1, size=length - pattern_start)) + [self.atr[0]]
            self.order_df["TENKAN_SEN"] = list(self.ichimoku.tenkan_sen.get(ago=-1, size=length - pattern_start)) + [
                self.ichimoku.tenkan_sen[0]]
            self.order_df["KIJUN_SEN"] = list(self.ichimoku.kijun_sen.get(ago=-1, size=length - pattern_start)) + [
                self.ichimoku.kijun_sen[0]]
            self.order_df["SENKOU_A"] = list(self.ichimoku.senkou_span_a.get(ago=-1, size=length - pattern_start)) + [
                self.ichimoku.senkou_span_a[0]]
            self.order_df["SENKOU_B"] = list(self.ichimoku.senkou_span_b.get(ago=-1, size=length - pattern_start)) + [
                self.ichimoku.senkou_span_b[0]]

            # Address optional order conditions

            # IMPL ADX_SIMPLE
            if self.adx_simple and self.adx[0] >= 45:
                self.reset_order_data()
                return

            # IMPL ADX_LOOKBACK
            if self.adx_lookback:
                index = -1
                for i, adx_val in enumerate(self.order_df["ADX"]):
                    if adx_val >= 45:
                        index = i
                if index == len(self.order_df["ADX"]) - 1:
                    self.reset_order_data()
                    return

            # IMPL RSI_RANGE
            if self.rsi_range:
                if self.rsi[0] > 66.66 or self.rsi[0] < 33.33:
                    self.reset_order_data()
                    return

            # Place order if all conditions are met
            print("PLACING ORDER")
            if not self.trailing_stop:
                self.order = self.buy_bracket(**args) if self.demark.lines.buy_countdowns[0] else self.sell_bracket(**args)
            else:
                breakpoint()
                self.order = self.buy(**args) if self.demark.lines.buy_countdowns[0] else self.sell(**args)



        elif (self.demark.lines.buy_countdowns[0] or self.demark.lines.sell_countdowns[0]) and self.order_df is not None:

            print("Conflict detected")

            # IMPL CLOSE_ON_CONFLICT
            if self.close_on_conflict:
                if self.order_params["type"] == "BUY" and self.demark.lines.sell_countdowns[0]:
                    print("Closing BUY on conflict")
                    self.coc_flag = True
                    self.close()
                    self.order[1].reject()
                    self.order[2].reject()
                    # breakpoint()
                elif self.order_params["type"] == "SELL" and self.demark.lines.buy_countdowns[0]:
                    print("Closing SELL on conflict")
                    self.coc_flag = True
                    self.close()
                    # 736312.9166666666
                    # breakpoint()
                    self.order[1].reject()
                    self.order[2].reject()

    def write_pattern(self, dataframe: pd.DataFrame, metadata) -> None:
        """
        Writes the dataframe to a csv, and the label (and metadata) to a json file with .label extension

        Args:
            output_directory (str): Supplied output directory
            dataframe (pd.DataFrame): Pandas dataframe
            metadata (dict) : Object which includes label

        Returns:
            (None)
        """

        if self.outputdir is None:
            return

        orderid = metadata["posix_time"]
        outname = self.outputdir + "/" + str(orderid)
        dataframe.to_csv(outname + ".csv", index=False)

        metadata_df = pd.DataFrame(data=metadata, index=[0])
        metadata_df.to_csv(outname + ".meta", index=False)
        return

    def reset_order_data(self):
        """
        Reset / clear all the order data. Needs to be done when an order is
         
            closed (e.g. for stoploss / take profit) or when an order is cancelled
        """
        # self.order = None
        self.order_df = None
        self.coc_flag = False
        self.order_params.clear()
