import unittest
import sys
import logging
from symphony.tests_v2.utils import dummy_td_countdown_data
from symphony.indicator_v2 import IndicatorRegistry
from symphony.indicator_v2.demark import bullish_price_flip, bearish_price_flip
from symphony.indicator_v2.demark import td_sell_setup, td_buy_setup, td_buy_countdown, td_sell_countdown
from symphony.indicator_v2.demark import td_buy_9_13_9, td_sell_9_13_9
import pandas as pd

dummy_price_history = dummy_td_countdown_data()
bearish_price_flip(dummy_price_history)
bullish_price_flip(dummy_price_history)
td_buy_setup(dummy_price_history)
td_sell_setup(dummy_price_history)
td_buy_countdown(dummy_price_history)
td_sell_countdown(dummy_price_history)

# TODO: Find test data
class TD9139Test(unittest.TestCase):


    def test_td_buy_9_13_9(self):

        df = td_buy_9_13_9(dummy_price_history, price_history_copy=True).price_history

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_td_sell_9_13_9(self):

        df = td_sell_9_13_9(dummy_price_history, price_history_copy=True).price_history

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("TD9139Test.test_td_buy_9_13_9").setLevel(logging.DEBUG)
    logging.getLogger("TD9139Test.test_td_sell_9_13_9").setLevel(logging.DEBUG)
    unittest.main()
