from symphony.indicator_v2.demark import td_upwave, td_downwave, td_buy_setup, td_sell_setup, td_buy_countdown, td_sell_countdown, td_buy_9_13_9, td_sell_9_13_9, \
    bullish_price_flip, bearish_price_flip, td_buy_combo, td_sell_combo
import backtrader as bt
from symphony.data_classes import PriceHistory
from symphony.config import USE_MODIN
from typing import Optional

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

class DemarkWrapper(bt.Indicator):
    lines = ('bullish_price_flip', 'bearish_price_flip', 'buy_setup', 'perfect_buy_setup', 'tdst_resistance',
             'buy_setup_true_end_index', 'sell_setup', 'perfect_sell_setup', 'tdst_support', 'sell_setup_true_end_index',
             'buy_countdown', 'aggressive_buy_countdown', 'pattern_start_index', 'sell_countdown', 'aggressive_sell_countdown',
             'buy_combo', 'sell_combo', 'buy_9_13_9', 'sell_9_13_9', 'dwave_up', 'dwave_down')
    params = (('period', 6),)

    def __init__(self, price_history: Optional[PriceHistory] = None):
        self.addminperiod(self.params.period)
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
        self.price_history = price_history
        return

    def next(self):
        for key in self.price_history.price_history.columns.tolist()[5:]:
            index = pd.Timestamp(self.data.datetime.datetime(), tz='utc', unit='s')
            getattr(self.lines, key)[0] = self.price_history.price_history.loc[index][key]
        return


