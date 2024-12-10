import unittest
import json
import os
import sys
import logging
from pprint import pprint
from os.path import join, dirname
import fastjsonschema
from symphony.tradingutils.candles.candles_converter import CandlesConverter
from symphony.tests.suite_utils import TestingUtils
import symphony.config.env as env_config

class CandlesConverterTest(unittest.TestCase):
    price_history = TestingUtils.dummy_price_history()
    def test_merged_tradingview(self):
        price_history = TestingUtils.dummy_price_history()
        
        self.assertEquals(price_history["price_history"][-1]["candle"]["close"], 1.10181)
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
    
    def test_to_csv(self):
        CandlesConverter.price_history_to_csv(self.price_history, env_config.TEST_DIR + "data/test_data.csv") #, datetime_format="%Y-%m-%d %H:%M:%S")
        
if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "CandlesConverterTest.test_merged_tradingview" ).setLevel( logging.DEBUG )
    logging.getLogger( "CandlesConverterTest.test_to_csv" ).setLevel( logging.DEBUG )

    unittest.main()