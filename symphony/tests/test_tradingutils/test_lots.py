import unittest
import json
import os
import sys
import logging
from pprint import pprint
from os.path import join, dirname
from symphony.tradingutils.lots import Lot
from symphony.tests.suite_utils import TestingUtils
from symphony.exceptions.trading_utils_exception import UnknownLotName, UnknownLotSize, UnquotablePipValue 

class LotsTest(unittest.TestCase):
    
    def test_lots(self):
        lot = Lot(100000)
        self.assertEquals(lot.lot_size, 100000)
        self.assertEquals(lot.lot_name, "standard")
        
        with self.assertRaises(UnknownLotSize):
            Lot(1000000)
            
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
    
    def test_pip_value_counter_currency(self):
        lot = Lot(100000)
        pip_value = lot.pip_value("EURUSD")
        self.assertEquals(pip_value, 10.)
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
        
    def test_pip_value_base_currency(self):
        lot = Lot(100000)
        
        with self.assertRaises(UnquotablePipValue):
            lot.pip_value("USDCHF")
            
        pip_value = lot.pip_value("USDCHF", price=1.4555)
        self.assertEquals(pip_value, 6.87)
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
        
if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "LotsTest.test_lots" ).setLevel( logging.DEBUG )
    logging.getLogger( "LotsTest.test_pip_value_counter_currency" ).setLevel( logging.DEBUG )
    logging.getLogger( "LotsTest.test_pip_base_counter_currency" ).setLevel( logging.DEBUG )

    unittest.main()