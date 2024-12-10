import backtrader as bt
from symphony.enum import Market
from symphony.data_classes import PriceHistory
from symphony.backtest_v2.indicators import DemarkWrapper
from symphony.indicator_v2.indicator_registry import IndicatorRegistry
from symphony.indicator_v2.demark.helpers import td_stoploss
from symphony.risk_management import CryptoPositionSizer
from symphony.config import USE_MODIN
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


class DemarkCountdownSwingStrategy(bt.Strategy):

    def __init__(self, price_history: PriceHistory):
        self.price_history = price_history
        self.demark = DemarkWrapper(price_history=price_history)
        self.risk_perc = 0.02
        self.order = None

    def stop(self):
        pass

    def notify_order(self, order):
        print(order.status)
        if order.status == order.Completed:
            breakpoint()

    def notify_trade(self, trade):
        print("trade")
        breakpoint()
        if trade.isclosed:
            breakpoint()
            self.order = None
            pass

    def next(self):
        if self.demark.buy_setup[0]:
            if not self.order:
                print(f"Found buy setup at {self.data.datetime.datetime()}")
                self.order = self.__place_order(IndicatorRegistry.BUY_SETUP)

        if self.demark.sell_setup[0]:
            print(f"Found sell setup at {self.data.datetime.datetime()}")
            if self.order:
                print(f"Closing order because of sell setup")
                self.order = None
                self.close()

    def __place_order(self, pattern: str):

        if pattern in [IndicatorRegistry.BUY_SETUP, IndicatorRegistry.BUY_COUNTDOWN, IndicatorRegistry.BUY_COMBO, IndicatorRegistry.AGGRESSIVE_BUY_COUNTDOWN, IndicatorRegistry.BUY_9_13_9]:
            order_type = Market.BUY
        else:
            order_type = Market.SELL

        index = pd.Timestamp(self.data.datetime.datetime(), tz='utc', unit='s')
        stop_loss = td_stoploss(self.price_history, IndicatorRegistry.BUY_SETUP, index)
        position_size = CryptoPositionSizer.simple_position_size(
            order_type,
            self.data.open[0],
            stop_loss,
            self.broker.get_cash(),
            self.risk_perc,
            self.price_history.instrument.digits
        )
        args = {
            "stopprice": stop_loss,
            "exectype": bt.Order.Market,
            "size": 1,
            "tradeid": 1111
        }
        print(f"{order_type}, {args}")
        if order_type == Market.BUY:
            return self.buy(**args)
        else:
            return self.sell(**args)