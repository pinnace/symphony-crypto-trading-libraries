import unittest
import sys
import logging
from symphony.tests_v2.utils import dummy_td_countdown_data
from symphony.indicator_v2.oscillators import zig_zag
from symphony.indicator_v2 import IndicatorRegistry
import pandas as pd

dummy_price_history = dummy_td_countdown_data()


class ZigZagTest(unittest.TestCase):

    def test_zig_zag(self):
        zig_zag(dummy_price_history)
        breakpoint()
        self.assertIn(IndicatorRegistry.ZIGZAG.value, dummy_price_history.price_history.columns)
        self.assertIn(IndicatorRegistry.ZIGZAG_REPAINT.value, dummy_price_history.price_history.columns)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("DerivativeOscillatorTest.test_derivative_oscillator").setLevel(logging.DEBUG)
    unittest.main()