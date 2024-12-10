import unittest
import sys
import logging
from symphony.data_classes import PriceHistory


class PriceHistoryTest(unittest.TestCase):

    def test_price_history(self):
        ph = PriceHistory()
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("PriceHistoryTest.test_price_history").setLevel(logging.DEBUG)
    unittest.main()
