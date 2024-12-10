import unittest
import json
import os
import sys
import logging
from pprint import pprint
from os.path import join, dirname
from symphony.tradingutils.currencies import Currency
from symphony.tests.suite_utils import TestingUtils


class CurrencyTest(unittest.TestCase):
    
    def test_currencies(self):
        c = Currency("EURUSD")
        self.assertEquals(c.base, "EUR")
        self.assertEquals(c.counter, "USD")
        self.assertEquals(c.currency_pair, "EURUSD")
        self.assertAlmostEquals(c.digits, 5)
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
        
if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "CurrencyTest.test_currencies" ).setLevel( logging.DEBUG )

    unittest.main()