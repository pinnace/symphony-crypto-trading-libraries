import unittest
import json
import os
import sys
import logging
from pprint import pprint
from os.path import join, dirname
import fastjsonschema
import numpy as np
from symphony.schema.schema_kit import SchemaKit
from symphony.indicators.demarkcountdown.td_price_flip import bullish_price_flip, bearish_price_flip
from symphony.tradingutils.candles.candles_converter import CandlesConverter
from symphony.indicators.indicator_kit import IndicatorKit
from symphony.tests.suite_utils import TestingUtils


class DemarkPriceFlipTest(unittest.TestCase):
    price_history: dict = TestingUtils.dummy_price_history()

    def test_bullish_price_flip(self):
        # Convert it for numpy
        price_history = self.price_history
        flow = SchemaKit.standard_flow()
        flow["price_history"] = IndicatorKit.price_history_to_arr(price_history)

        # Call the bullish_price_flip indicator
        flips = bullish_price_flip(flow)
        # This flips should all correspond with setup index 1
        for index, flip in enumerate(flips["bullish_price_flips"]):
            #TODO: Create an indicator component to the price history object
            #TODO: Validate the demark struct against schema
            if flip:
                self.assertEqual(price_history["price_history"][index]["comment"]["setup_index"], 1)
                self.assertEqual(price_history["price_history"][index]["comment"]["setup_type"], 'SELL')
        
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")

    def test_bearish_price_flip(self):
        # Convert it for numpy
        price_history = self.price_history
        flow = SchemaKit.standard_flow()
        flow["price_history"] = IndicatorKit.price_history_to_arr(price_history)
        
        # Call the bullish_price_flip indicator
        flips = bearish_price_flip(flow)

        # This flips should all correspond with setup index 1
        for index, flip in enumerate(flips["bearish_price_flips"]):
            #TODO: Create an indicator component to the price history object
            #TODO: Validate the demark struct against schema
            if flip:
                self.assertEqual(price_history["price_history"][index]["comment"]["setup_index"], 1)
                self.assertEqual(price_history["price_history"][index]["comment"]["setup_type"], 'BUY')
        
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
    


if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "DemarkPriceFlipTest.test_bullish_price_flip" ).setLevel( logging.DEBUG )
    logging.getLogger( "DemarkPriceFlipTest.test_bearish_price_flip" ).setLevel( logging.DEBUG )
    unittest.main()