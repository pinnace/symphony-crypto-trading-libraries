import unittest
import sys
import logging
from symphony.tests_v2.utils import dummy_td_countdown_data
from symphony.indicator_v2.oscillators import derivative_oscillator
from symphony.indicator_v2 import IndicatorRegistry
import pandas as pd

dummy_price_history = dummy_td_countdown_data()


class DerivativeOscillatorTest(unittest.TestCase):

    def test_derivative_oscillator(self):
        derivative_oscillator(dummy_price_history)
        self.assertIn(IndicatorRegistry.DERIVATIVE_OSCILLATOR.value, dummy_price_history.price_history.columns)
        self.assertIn(IndicatorRegistry.DERIVATIVE_OSCILLATOR_SIGNAL.value, dummy_price_history.price_history.columns)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("DerivativeOscillatorTest.test_derivative_oscillator").setLevel(logging.DEBUG)
    unittest.main()