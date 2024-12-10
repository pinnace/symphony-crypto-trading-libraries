import unittest
import sys
import logging
from symphony.tests_v2.utils import dummy_td_countdown_data
from symphony.indicator_v2.candlestick import candlesticks
from symphony.indicator_v2 import IndicatorRegistry
import pandas as pd

dummy_price_history = dummy_td_countdown_data()


class CandlestickTest(unittest.TestCase):

    def test_candlesticks(self):
        candlesticks(dummy_price_history)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("CandlestickTest.test_candlesticks").setLevel(logging.DEBUG)
    unittest.main()