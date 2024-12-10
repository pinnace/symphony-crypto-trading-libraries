import unittest
import json
import os
import sys
import logging
from pprint import pprint
from os.path import join, dirname
from symphony.tradingutils.timeframes import Timeframe
from symphony.exceptions.trading_utils_exception import UnknownTimeframe
from symphony.tests.suite_utils import TestingUtils


class TimeframeTest(unittest.TestCase):
    
    def test_timeframe_std(self):
        tf = Timeframe("M1")
        self.assertEquals(tf.std, "1")
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
        
    def test_timeframe_str(self):
        tf = Timeframe("1440")
        self.assertEquals(tf.str, "D1")
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
    
    def test_timeframe_exception(self):
        
        with self.assertRaises(UnknownTimeframe):
            tf = Timeframe("G6")
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
        
if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "TimeframeTest.test_timeframe_std" ).setLevel( logging.DEBUG )
    logging.getLogger( "TimeframeTest.test_timeframe_str" ).setLevel( logging.DEBUG )
    logging.getLogger( "TimeframeTest.test_timeframe_exception" ).setLevel( logging.DEBUG )

    unittest.main()