import unittest
import json
import os
import sys
import logging
from pprint import pprint
from symphony.indicators.indicator_registry import IndicatorRegistry
from symphony.indicators.indicator_flow import IndicatorFlow
from symphony.tests.suite_utils import TestingUtils



class IndicatorFlowTest(unittest.TestCase):
    price_history = TestingUtils.dummy_price_history()
    
    def test_indicator_flow_price_flips_no_args(self):
        price_history = self.price_history
        indicators = [IndicatorRegistry.BULLISH_PRICE_FLIP]
        indicator_settings = [
            {"window_size" : 6}
        ]

        indicator_flow = IndicatorFlow(price_history, indicators, indicator_settings)


        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")

        
        

        


if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "IndicatorFlowTest.test_indicator_flow_price_flips_no_args" ).setLevel( logging.DEBUG )
    unittest.main()