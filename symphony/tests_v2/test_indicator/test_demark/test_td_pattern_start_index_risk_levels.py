import unittest
import sys
import logging
from symphony.tests_v2.utils import dummy_td_countdown_data
from symphony.indicator_v2 import IndicatorRegistry
from symphony.indicator_v2.demark import bullish_price_flip, bearish_price_flip
from symphony.indicator_v2.demark import td_sell_setup, td_buy_setup
from symphony.indicator_v2.demark import td_buy_countdown, td_sell_countdown, td_buy_combo, td_sell_combo
from symphony.indicator_v2.demark import td_buy_9_13_9, td_sell_9_13_9
import pandas as pd
from typing import List

dummy_price_history = dummy_td_countdown_data()
bearish_price_flip(dummy_price_history)
bullish_price_flip(dummy_price_history)
td_buy_setup(dummy_price_history)
td_sell_setup(dummy_price_history)


class TDPatternStart(unittest.TestCase):

    def test_pattern_start_all(self):
        price_history = dummy_price_history
        price_history = td_buy_countdown(price_history)
        price_history = td_sell_countdown(price_history)
        price_history = td_buy_combo(price_history)
        price_history = td_sell_combo(price_history)
        price_history = td_buy_9_13_9(price_history)
        price_history = td_sell_9_13_9(price_history)
        df = price_history.price_history

        buy_countdowns: List[pd.Timestamp] = df.index[df[IndicatorRegistry.BUY_COUNTDOWN.value] == 1].tolist()
        sell_countdowns: List[pd.Timestamp] = df.index[df[IndicatorRegistry.SELL_COUNTDOWN.value] == 1].tolist()
        agg_buy_countdowns: List[pd.Timestamp] = df.index[df[IndicatorRegistry.AGGRESSIVE_BUY_COUNTDOWN.value] == 1].tolist()
        agg_sell_countdowns: List[pd.Timestamp] = df.index[df[IndicatorRegistry.AGGRESSIVE_SELL_COUNTDOWN.value] == 1].tolist()
        combo_buy_countdowns: List[pd.Timestamp] = df.index[df[IndicatorRegistry.BUY_COMBO.value] == 1].tolist()
        combo_sell_countdowns: List[pd.Timestamp] = df.index[df[IndicatorRegistry.SELL_COMBO.value] == 1].tolist()
        buy_9_13_9s: List[pd.Timestamp] = df.index[df[IndicatorRegistry.BUY_9_13_9.value] == 1].tolist()
        sell_9_13_9s: List[pd.Timestamp] = df.index[df[IndicatorRegistry.SELL_9_13_9.value] == 1].tolist()

        for buy_countdown in buy_countdowns:
            self.assertNotEquals(df[IndicatorRegistry.PATTERN_START_INDEX.value].loc[buy_countdown], 0)
        for sell_countdown in sell_countdowns:
            self.assertNotEquals(df[IndicatorRegistry.PATTERN_START_INDEX.value].loc[sell_countdown], 0)
        for agg_buy_countdown in agg_buy_countdowns:
            self.assertNotEquals(df[IndicatorRegistry.PATTERN_START_INDEX.value].loc[agg_buy_countdown], 0)
        for agg_sell_countdown in agg_sell_countdowns:
            self.assertNotEquals(df[IndicatorRegistry.PATTERN_START_INDEX.value].loc[agg_sell_countdown], 0)
        for combo_buy_countdown in combo_buy_countdowns:
            self.assertNotEquals(df[IndicatorRegistry.PATTERN_START_INDEX.value].loc[combo_buy_countdown], 0)
        for combo_sell_countdown in combo_sell_countdowns:
            self.assertNotEquals(df[IndicatorRegistry.PATTERN_START_INDEX.value].loc[combo_sell_countdown], 0)
        for buy_9_13_9 in buy_9_13_9s:
            self.assertNotEquals(df[IndicatorRegistry.PATTERN_START_INDEX.value].loc[buy_9_13_9], 0)
        for sell_9_13_9 in sell_9_13_9s:
            self.assertNotEquals(df[IndicatorRegistry.PATTERN_START_INDEX.value].loc[sell_9_13_9], 0)

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

class TDRiskLevels(unittest.TestCase):

    def test_countdown_stoploss_takeprofit(self):


        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("TDPatternStart.test_pattern_start_all").setLevel(logging.DEBUG)
    logging.getLogger("TDRiskLevels.test_countdown_stoploss_takeprofit").setLevel(logging.DEBUG)
    unittest.main()
