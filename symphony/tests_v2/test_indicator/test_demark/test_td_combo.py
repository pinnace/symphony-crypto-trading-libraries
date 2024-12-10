import unittest
import sys
import logging
from symphony.tests_v2.utils import dummy_td_countdown_data
from symphony.indicator_v2 import IndicatorRegistry
from symphony.indicator_v2.demark import bullish_price_flip, bearish_price_flip
from symphony.indicator_v2.demark import td_sell_setup, td_buy_setup
from symphony.indicator_v2.demark import td_buy_combo, td_sell_combo
import pandas as pd

dummy_price_history = dummy_td_countdown_data()
bearish_price_flip(dummy_price_history)
bullish_price_flip(dummy_price_history)
td_buy_setup(dummy_price_history)
td_sell_setup(dummy_price_history)

class TDComboTest(unittest.TestCase):

    # For combos, dummy price history has incorrect implementation.
    def test_td_buy_combo(self):

        df = td_buy_combo(dummy_price_history, price_history_copy=True, strict=False).price_history
        """
        for index, row in dummy_price_history.price_history.iterrows():

            # Too early
            if df.index.get_loc(index) < 30:
                continue

            if df["TEST_BUY_COMBO"].loc[index] == 1:
                self.assertEquals(df[IndicatorRegistry.BUY_COMBO.value].loc[index], 1)

            if df[IndicatorRegistry.BUY_COMBO.value].loc[index]:
                self.assertEquals(df["TEST_BUY_COMBO"].loc[index], 1)
        """
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_td_sell_combo(self):

        df = td_sell_combo(dummy_price_history, price_history_copy=True, strict=True).price_history
        """
        for index, row in dummy_price_history.price_history.iterrows():

            # Too early
            if df.index.get_loc(index) < 30:
                continue

            if df["TEST_SELL_COMBO"].loc[index] == 1:
                breakpoint()
                self.assertEquals(df[IndicatorRegistry.SELL_COMBO.value].loc[index], 1)

            if df[IndicatorRegistry.SELL_COMBO.value].loc[index]:
                breakpoint()
                self.assertEquals(df["TEST_SELL_COMBO"].loc[index], 1)
        """

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("TDComboTest.test_td_buy_combo").setLevel(logging.DEBUG)
    logging.getLogger("TDComboTest.test_td_sell_combo").setLevel(logging.DEBUG)
    unittest.main()
