import unittest
import sys
import logging
from symphony.tests_v2.utils import dummy_td_countdown_data
from symphony.indicator_v2 import IndicatorRegistry
from symphony.indicator_v2.demark import td_range_expansion_index
import pandas as pd

dummy_price_history = dummy_td_countdown_data()

class TDREITest(unittest.TestCase):

    # For combos, dummy price history has incorrect implementation.
    def test_td_range_expansion_index(self):
        td_range_expansion_index(dummy_price_history)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("TDREITest.test_td_range_expansion_index").setLevel(logging.DEBUG)
    unittest.main()
