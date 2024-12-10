from datetime import datetime
import backtrader as bt
from symphony.backtest.indicators import DemarkCountdownBtIndicatorWrapper
from symphony.indicators.demarkcountdown.td_utils import get_td_countdown_pattern_start, get_td_countdown_stoploss, \
    get_td_pattern_ohclv
from symphony.indicators import IndicatorKit, IndicatorRegistry
from symphony.schema import SchemaKit
from symphony.risk_management.historical_rate_converter import HistoricalRatesConverter
from symphony.tradingutils.timeframes import Timeframe
from symphony.tradingutils.currencies import Currency
from symphony.exceptions.backtester_exception import BacktesterError
import pathlib
import pandas as pd


# Create a subclass of Strategy to define the indicators and logic

class DemarkCountdownStrategyV2(bt.Strategy):
    # list of parameters which are configurable for the strategy
    demark_line_names = (
        "bearish_price_flips", "buy_setups", "buy_setups_true_end", "perfect_buy_setups", "tdst_resistance",
        "active_buy_setups", "buy_countdowns", "bullish_price_flips", "sell_setups", "sell_setups_true_end",
        "perfect_sell_setups", "tdst_support", "active_sell_setups", "sell_countdowns", "tradeable_buy_setups",
        "tradeable_sell_setups",)

    def __init__(self, instrument, timeframe, risk_perc=0.02, outputdir=None,
                 cancellation_qualifier_I=False,  #
                 cancellation_qualifier_II=False,  #
                 adx_simple=False,  #
                 adx_lookback=False,  #
                 rsi_range=False,  #
                 atr_stop=False,
                 trailing_stop=False,  #
                 fib_take_profit_level=1.0,  #
                 close_on_conflict=False,
                 ):

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

        self.demark = DemarkCountdownBtIndicatorWrapper(cancellation_qualifier_I=self.cancellation_qualifier_I,
                                                        cancellation_qualifier_II=self.cancellation_qualifier_II)
        self.rsi = bt.talib.RSI()
        self.adx = bt.indicators.DirectionalMovementIndex()
        self.atr = bt.indicators.ATR()
        self.ichimoku = bt.indicators.Ichimoku()

        self.order = None
        self.order_params = {}
        self.order_df = None
        self.flow = SchemaKit.standard_flow()
        self.currency = Currency(instrument)
        self.timeframe = Timeframe(timeframe)
        self.outputdir = outputdir
        if self.outputdir is not None:
            path = pathlib.Path(self.outputdir)
            path.mkdir(parents=True, exist_ok=True)

        self.risk_perc = risk_perc
        self.digits = 5
        self.hrc = HistoricalRatesConverter('Oanda', self.timeframe.std, self.currency.currency_pair, 1000)

        self.total_profit = 0
        self.possible_buy_trades = 0
        self.possible_sell_trades = 0
        self.sell_trade_count = 0
        self.buy_trade_count = 0
        self.profitable_sell_trade_count = 0
        self.profitable_buy_trade_count = 0

    def stop(self):
        # Record all possible trades
        self.populate_flow()
        self.possible_buy_trades = self.flow["indicators"][0]['data']['buy_countdowns'].count(1.0)
        self.possible_sell_trades = self.flow["indicators"][0]['data']['sell_countdowns'].count(1.0)

        print("Possible BUYs: {}, Possible Sells: {}".format(self.possible_buy_trades, self.possible_sell_trades))
        print("[{}|{}] P/L: {}".format(
            self.currency.currency_pair,
            self.timeframe.std,
            round(self.total_profit, 2)))

    def notify_trade(self, trade):
        if trade.isclosed:
            profit = trade.pnl
            self.total_profit += profit
            if profit > 0.0:
                if self.order_params["type"].lower() == "buy":
                    self.profitable_buy_trade_count += 1
                if self.order_params["type"].lower() == "sell":
                    self.profitable_sell_trade_count += 1

            print('Closing trade, gross {0:.2f}, net {1:.2f}. Account balance: {2:.2f}'.format(trade.pnl, trade.pnlcomm, self.broker.cash))
        elif trade.isopen:
            pass
        else:
            raise BacktesterError(__name__ + ": Unknown trade condition")

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        ex_price = order.executed.price
        # Left the market
        if not self.position:
            if not order.status == order.Canceled:
                if order.executed.price == self.order_params["stop_loss"]:
                    print("[+] Hit stoploss")
                elif order.executed.price == self.order_params["take_profit"]:
                    print("[+] Hit takeprofit")
                else:
                    print("[-] Closing something")

                # Record trade
                profit_in_pips = int(
                    (order.executed.price - self.order_params["open_price"])
                    * (10 ** self.currency.digits))
                metadata = self.get_metadata(profit_in_pips)
                self.write_pattern(self.order_df, metadata)

            return
        # Entered the market
        if order.status in [order.Completed]:
            if order.isbuy():
                print("[+] Enter BUY ")
                self.buy_trade_count += 1
            elif order.issell():
                print("[+] Enter SELL")
                self.sell_trade_count += 1
            else:
                print("[-] Unknown")

    def next(self):

        if self.position:
            if self.demark.lines.buy_countdowns[0] or self.demark.lines.sell_countdowns[0]:
                if self.close_on_conflict:
                    order_type = self.order_params["type"].lower()
                    if order_type != "buy" and order_type != "sell":
                        raise BacktesterError(
                            __name__ + ": Error in Close-on-conflict, type: {}".format(self.order_params["type"]))

                    if order_type == "buy" and self.demark.lines.sell_countdowns[0]:
                        print("[+] Found countdown in opposite direction to BUY")
                        self.close()
                    elif order_type == "sell" and self.demark.lines.buy_countdowns[0]:
                        print("[+] Found countdown in opposite direction to SELL")
                        self.close()
                    else:
                        pass

        if not self.position:  # not in the market
            if self.demark.lines.buy_countdowns[0] or self.demark.lines.sell_countdowns[0]:

                current_dt = lambda: self.data.datetime.date().strftime('%Y-%m-%d %H:%M:%S')
                self.populate_flow()
                length = len(self.data.close) - 1
                pattern_start = get_td_countdown_pattern_start(self.flow, length)
                pattern = get_td_pattern_ohclv(self.flow, pattern_start)



                stop_loss = self.get_stop_loss()
                take_profit = self.get_take_profit()

                # IMPL ATR Stop & FIP TP
                take_profit = self.apply_fib_tp()
                if self.atr_stop:
                    stop_loss = self.apply_atr_stop(stop_loss, take_profit)

                # Store the order parameters
                self.order_params = self.record_order(stop_loss, take_profit)

                # Edge case where the stoploss is equal to the open price
                if self.order_params["stop_in_pips"] == 0:
                    print("[!] Edge case, Stop in pips is 0.")
                    return
                # Calculate position size
                units = self.hrc.get_units(self.risk_perc, self.broker.get_cash(), self.order_params["stop_in_pips"],
                                           current_dt())
                units *= 10

                if self.trailing_stop:
                    trailamount = round(
                        abs(self.data.close[0] - stop_loss),
                        self.digits)
                    # trailpercent = round(trailamount / self.order_params["open_price"], 5)

                    args = {"exectype": bt.Order.StopTrail, "limitprice": take_profit, "trailamount": trailamount,
                            "size": units, "price": self.order_params["open_price"]}
                else:
                    args = {"limitprice": take_profit, "stopprice": stop_loss, "exectype": bt.Order.Market,
                            "size": units}

                print("Placing {} order at - {} - with price: {}".format(self.order_params["type"], current_dt(),
                                                                         self.order_params["open_price"]))
                print("Placing order with params: {}".format(self.order_params))
                print("Placing order with args: {}".format(args))

                # Strategy conditions
                if not self.adx_simple_should_trade():
                    return
                if not self.adx_lookback_should_trade(length, pattern_start):
                    return
                if not self.rsi_range_should_trade():
                    return

                if self.demark.lines.buy_countdowns[0]:
                    self.order_df = self.get_order_df()
                    self.order = self.buy_bracket(**args)
                elif self.demark.lines.sell_countdowns[0]:
                    self.order_df = self.get_order_df()
                    self.order = self.sell_bracket(**args)
                else:
                    raise BacktesterError(__name__ + ": No countdown found when executing trade")
                # print(self.order)

    def populate_flow(self):
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

    def get_stop_loss(self):
        length = len(self.data.close) - 1
        return get_td_countdown_stoploss(self.flow, length)

    def get_take_profit(self):
        return self.demark.lines.tdst_resistance[0] if self.demark.lines.buy_countdowns[0] else \
            self.demark.lines.tdst_support[0]

    def record_order(self, stoploss, takeprofit):
        stop_in_pips = int(abs((self.data.close[0] - stoploss) * pow(10, self.currency.digits)))
        open_price = round(self.data.close[0], self.currency.digits)
        if abs(open_price - stoploss) > 0:
            profit_factor = abs(
                open_price - takeprofit) / abs(
                open_price - stoploss)
        else:
            profit_factor = 0.0
        return {
            "type": "buy" if stoploss < takeprofit else "sell",
            "open_price": open_price,
            "stop_loss": round(stoploss, self.currency.digits),
            "stop_in_pips": stop_in_pips,
            "take_profit": round(takeprofit, self.currency.digits),
            "profit_factor": profit_factor,
            "datetime": self.data.datetime.datetime().strftime('%Y-%m-%d %H:%M:%S'),
            "posix_time": int(self.data.datetime.datetime().timestamp())

        }

    def get_metadata(self, profit_in_pips: int):
        profitable = 1 if profit_in_pips > 0 else 0
        return {
            "profitable": 1 if profitable else 0,
            "profit_in_pips": profit_in_pips,
            "profit_factor": self.order_params["profit_factor"],
            "instrument": self.currency.currency_pair,
            "order_type": self.order_params["type"],
            "datetime": self.order_params["datetime"],
            "posix_time": self.order_params["posix_time"]
        }

    def get_order_df(self):
        length = len(self.data.close) - 1
        pattern_start = get_td_countdown_pattern_start(self.flow, length)
        pattern = get_td_pattern_ohclv(self.flow, pattern_start)
        order_df = pd.DataFrame(data=pattern)

        # Get some additional indicators
        order_df["RSI"] = list(self.rsi.get(ago=-1, size=length - pattern_start)) + [self.rsi[0]]
        order_df["ADX"] = list(self.adx.get(ago=-1, size=length - pattern_start)) + [self.adx[0]]
        order_df["PLUS_DI"] = list(self.adx.DIminus.get(ago=-1, size=length - pattern_start)) + [
            self.adx.DIminus[0]]
        order_df["MINUS_DI"] = list(self.adx.DIplus.get(ago=-1, size=length - pattern_start)) + [
            self.adx.DIplus[0]]
        order_df["ATR"] = list(self.atr.get(ago=-1, size=length - pattern_start)) + [self.atr[0]]
        order_df["TENKAN_SEN"] = list(self.ichimoku.tenkan_sen.get(ago=-1, size=length - pattern_start)) + [
            self.ichimoku.tenkan_sen[0]]
        order_df["KIJUN_SEN"] = list(self.ichimoku.kijun_sen.get(ago=-1, size=length - pattern_start)) + [
            self.ichimoku.kijun_sen[0]]
        order_df["SENKOU_A"] = list(self.ichimoku.senkou_span_a.get(ago=-1, size=length - pattern_start)) + [
            self.ichimoku.senkou_span_a[0]]
        order_df["SENKOU_B"] = list(self.ichimoku.senkou_span_b.get(ago=-1, size=length - pattern_start)) + [
            self.ichimoku.senkou_span_b[0]]
        return order_df

    def apply_atr_stop(self, stoploss, takeprofit) -> float:
        stop_loss = stoploss
        if stop_loss < takeprofit:
            stop_loss = round(stop_loss - self.atr[0], self.digits)
        elif stop_loss > takeprofit:
            stop_loss = round(stop_loss + self.atr[0], self.digits)
        else:
            raise BacktesterError(__name__ + ": Unknown order type")
        return stop_loss

    def apply_fib_tp(self) -> float:
        take_profit = self.get_take_profit()
        stop_loss = self.get_stop_loss()
        open_price = round(self.data.close[0], self.currency.digits)
        price_tp_diff = abs(
            open_price - self.demark.lines.tdst_resistance[0]) * self.fib_take_profit_level
        if stop_loss < take_profit:
            take_profit = round(open_price + price_tp_diff, self.digits)
        elif stop_loss > take_profit:
            take_profit = round(open_price - price_tp_diff, self.digits)
        else:
            raise BacktesterError(__name__ + ": Unknown order type")
        return take_profit

    def adx_simple_should_trade(self) -> bool:
        if self.adx_simple and self.adx[0] >= 45:
            return False
        return True

    def adx_lookback_should_trade(self, length, pattern_start) -> bool:
        adx_points = list(self.adx.get(ago=-1, size=length - pattern_start)) + [self.adx[0]]
        if self.adx_lookback:
            index = -1
            for i, adx_val in enumerate(adx_points):
                if adx_val >= 45:
                    index = i
            if index == len(adx_points) - 1:
                return False
        return True

    def rsi_range_should_trade(self) -> bool:
        if self.rsi_range:
            if self.rsi[0] > 66.66 or self.rsi[0] < 33.33:
                return False
        return True

    def write_pattern(self, dataframe: pd.DataFrame, metadata) -> None:
        """
        Writes the dataframe to a csv, and the label (and metadata) to a csv file with .label extension

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

