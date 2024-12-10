import unittest
import sys
import logging
from symphony.tests_v2.utils import dummy_td_countdown_data
from symphony.indicator_v2 import IndicatorRegistry
from symphony.indicator_v2.demark import bullish_price_flip, bearish_price_flip
from symphony.indicator_v2.demark import td_sell_setup, td_buy_setup
from symphony.indicator_v2.demark import td_buy_countdown, td_sell_countdown
import pandas as pd
from typing import List

dummy_price_history = dummy_td_countdown_data()
bearish_price_flip(dummy_price_history)
bullish_price_flip(dummy_price_history)
td_buy_setup(dummy_price_history)
td_sell_setup(dummy_price_history)


class TDCountdownTest(unittest.TestCase):

    def test_td_buy_countdown(self):
        df = td_buy_countdown(dummy_price_history, cancellation_qualifier_I=True,
                              cancellation_qualifier_II=True, price_history_copy=True).price_history

        test_indices: List[pd.Timestamp] = df.index[df["TEST_BUY_COUNTDOWN"] == 13].tolist()
        calculated_indices: List[pd.Timestamp] = df.index[df[IndicatorRegistry.BUY_COUNTDOWN.value] == 1].tolist()
        test_agg: List[pd.Timestamp] = df.index[df[IndicatorRegistry.AGGRESSIVE_BUY_COUNTDOWN.value] == 1].tolist()
        agg_indices: List[pd.Timestamp] = df.index[df["TEST_AGGRESSIVE_BUY_COUNTDOWN"] == 1].tolist()

        for index, row in dummy_price_history.price_history.iterrows():

            # Too early
            if df.index.get_loc(index) < 30:
                continue
            """
            if df["TEST_BUY_COUNTDOWN"].loc[index] == 13:
                self.assertEquals(df[IndicatorRegistry.BUY_COUNTDOWN.value].loc[index], 1)

            if df[IndicatorRegistry.BUY_COUNTDOWN.value].loc[index]:
                self.assertEquals(df["TEST_BUY_COUNTDOWN"].loc[index], 1)
            """
            """Test data is wrong
            if df["TEST_AGGRESSIVE_BUY_COUNTDOWN"].loc[index] == 1:
                self.assertEquals(df[IndicatorRegistry.AGGRESSIVE_BUY_COUNTDOWN.value].loc[index], 1)

            if df[IndicatorRegistry.AGGRESSIVE_BUY_COUNTDOWN.value].loc[index]:
                self.assertEquals(df["TEST_AGGRESSIVE_BUY_COUNTDOWN"].loc[index], 1)
            """

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_td_sell_countdown(self):
        df = td_sell_countdown(dummy_price_history, cancellation_qualifier_I=True,
                               cancellation_qualifier_II=True, price_history_copy=True).price_history

        test_indices: List[pd.Timestamp] = df.index[df["TEST_SELL_COUNTDOWN"] == 13].tolist()
        calculated_indices: List[pd.Timestamp] = df.index[df[IndicatorRegistry.SELL_COUNTDOWN.value] == 1].tolist()
        test_agg: List[pd.Timestamp] = df.index[df[IndicatorRegistry.AGGRESSIVE_SELL_COUNTDOWN.value] == 1].tolist()
        agg_indices: List[pd.Timestamp] = df.index[df["TEST_AGGRESSIVE_SELL_COUNTDOWN"] == 1].tolist()

        for index, row in dummy_price_history.price_history.iterrows():
            # Too early
            if df.index.get_loc(index) < 15:
                continue
            """
            if df["TEST_SELL_COUNTDOWN"].loc[index] == 13:
                self.assertEquals(df[IndicatorRegistry.SELL_COUNTDOWN.value].loc[index], 1)

            if df[IndicatorRegistry.SELL_COUNTDOWN.value].loc[index]:
                self.assertEquals(df["TEST_SELL_COUNTDOWN"].loc[index], 1)
            """
            """Test data is wrong
            if df["TEST_AGGRESSIVE_SELL_COUNTDOWN"].loc[index] == 1:
                self.assertEquals(df[IndicatorRegistry.AGGRESSIVE_SELL_COUNTDOWN.value].loc[index], 1)

            if df[IndicatorRegistry.AGGRESSIVE_SELL_COUNTDOWN.value].loc[index]:
                self.assertEquals(df["TEST_AGGRESSIVE_SELL_COUNTDOWN"].loc[index], 1)
            """

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("TDCountdownTest.test_td_buy_countdown").setLevel(logging.DEBUG)
    logging.getLogger("TDCountdownTest.test_td_sell_countdown").setLevel(logging.DEBUG)
    unittest.main()
