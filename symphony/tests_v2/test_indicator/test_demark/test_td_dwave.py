import unittest
import sys
import logging
from symphony.tests_v2.utils import dummy_td_countdown_data
from symphony.indicator_v2 import IndicatorRegistry
from symphony.indicator_v2.demark import td_upwave, td_downwave
import pandas as pd
from typing import List

dummy_price_history = dummy_td_countdown_data()


class TDDWaveTest(unittest.TestCase):

    def test_td_upwave(self):
        td_upwave(dummy_price_history)

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_td_downwave(self):
        td_downwave(dummy_price_history)

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("TDDWaveTest.test_td_upwave").setLevel(logging.DEBUG)
    logging.getLogger("TDDWaveTest.test_td_downwave").setLevel(logging.DEBUG)
    unittest.main()
