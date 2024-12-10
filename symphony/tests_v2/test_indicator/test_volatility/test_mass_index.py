import unittest
import sys
import logging
from symphony.tests_v2.utils import dummy_td_countdown_data
from symphony.indicator_v2.volatility import mass_index
from symphony.indicator_v2 import IndicatorRegistry
import pandas as pd

dummy_price_history = dummy_td_countdown_data()


class MassIndexTest(unittest.TestCase):

    def test_mass_index(self):
        mass_index(dummy_price_history)
        self.assertIn(IndicatorRegistry.MASS_INDEX.value, dummy_price_history.price_history.columns)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("MassIndexTest.test_mass_index").setLevel(logging.DEBUG)
    unittest.main()