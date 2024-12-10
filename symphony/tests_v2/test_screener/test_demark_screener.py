import unittest
import sys
import logging
from symphony.screener import DemarkScreener
from symphony.enum import Exchange, Timeframe



class DemarkScreenerTest(unittest.TestCase):

    def test_demark_screener(self):

        dm_screener = DemarkScreener(Timeframe.D1, ['ETHBTC', 'ADABNB', 'BTCBUSD'])
        num_bars = 100
        dm_screener.fetch(Exchange.BINANCE, num_bars)
        dm_screener.process()
        dm_screener.filter(7)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("DemarkScreenerTest.test_demark_screener").setLevel(logging.DEBUG)
    unittest.main()
