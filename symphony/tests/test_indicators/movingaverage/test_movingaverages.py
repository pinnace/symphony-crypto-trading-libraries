import unittest
import json
import os
import sys
import logging
from pprint import pprint
from os.path import join, dirname
import fastjsonschema
import numpy as np
from symphony.tests.suite_utils import TestingUtils
from symphony.indicators import IndicatorKit
from symphony.indicators.indicator_registry import IndicatorRegistry
from symphony.indicators.indicators import Indicators



class TestMovingAverages(unittest.TestCase):
    price_history: dict = TestingUtils.dummy_price_history()
    flow: dict = TestingUtils.dummy_flow()

    def test_sma(self):
        flow = self.flow
        flow["indicators"].append(
            IndicatorKit.get_indicator_for_flow(
                IndicatorRegistry.SMA.name, 
                {"period" : 5}, 
                Indicators.sma(flow)
                )
        )
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")

    def test_ema(self):
        flow = self.flow
        flow["indicators"].append(
            IndicatorKit.get_indicator_for_flow(
                IndicatorRegistry.EMA.name, 
                {"period" : 5}, 
                Indicators.ema(flow)
                )
        )
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")

    


if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "TestMovingAverages.test_sma" ).setLevel( logging.DEBUG )
    unittest.main()