import unittest
import sys
import logging
from symphony.tests_v2.utils import dummy_td_countdown_data
from symphony.indicator_v2 import IndicatorRegistry
from symphony.indicator_v2.demark import bullish_price_flip, bearish_price_flip


class DemarkTDPriceFlipTest(unittest.TestCase):
    dummy_price_history = dummy_td_countdown_data()

    def test_bullish_price_flip(self):
        bullish_price_flip(self.dummy_price_history)
        for index, row in self.dummy_price_history.price_history.iterrows():

            if row[IndicatorRegistry.BULLISH_PRICE_FLIP.value]:
                self.assertEquals(
                    row[IndicatorRegistry.BULLISH_PRICE_FLIP.value],
                    row["TEST_SELL_SETUP"]
                )

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_bearish_price_flip(self):
        bearish_price_flip(self.dummy_price_history)
        for index, row in self.dummy_price_history.price_history.iterrows():

            if row[IndicatorRegistry.BEARISH_PRICE_FLIP.value]:
                self.assertEquals(
                    row[IndicatorRegistry.BEARISH_PRICE_FLIP.value],
                    row["TEST_BUY_SETUP"]
                )
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("DemarkTDPriceFlipTest.test_bullish_price_flip").setLevel(logging.DEBUG)
    logging.getLogger("DemarkTDPriceFlipTest.test_bearish_price_flip").setLevel(logging.DEBUG)
    unittest.main()