import unittest
import json
import os
import sys
import logging
from pprint import pprint
from os.path import join, dirname
from symphony.risk_management.historical_rate_converter import PositionSizer


class PositionSizerTest(unittest.TestCase):
    # https://www.babypips.com/learn/forex/calculating-position-sizes
    
    def test_ps_counter_currency(self):
       position_sizer = PositionSizer("EURUSD", 10000)
       self.assertEquals(position_sizer.get_units(0.01, 5000, 200), 2500)
       
       position_sizer = PositionSizer("USDJPY", 10000, account_denomination="JPY")
       self.assertEquals(position_sizer.get_units(0.02, 10000, 200), 10000)
       
       
       position_sizer = PositionSizer("EURGBP", 10000, account_denomination="GBP")
       self.assertEquals(position_sizer.get_units(0.05, 3459, 400), 4324)
       print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
       
    def test_ps_base_currency(self):
       position_sizer = PositionSizer("EURUSD", 10000, account_denomination="EUR")
       self.assertEquals(position_sizer.get_units(0.01, 5000, 200, price=1.5), 3750)
       
       position_sizer = PositionSizer("USDJPY", 100000)
       self.assertEquals(position_sizer.get_units(0.02, 10000, 200, price=106.61500), 10662)
       
       position_sizer = PositionSizer("USDCHF", 10000)
       self.assertEquals(position_sizer.get_units(0.02, 10000, 200, price=0.97440), 9744)
       
       
       position_sizer = PositionSizer("CHFJPY", 10000, account_denomination="CHF")
       self.assertEquals(position_sizer.get_units(0.05, 6785, 153, price=109.44), 24266)
       
       print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
    
    def test_ps_acct_denom_is_conv_counter(self):
       position_sizer = PositionSizer("EURGBP", 10000, account_denomination="USD")
       self.assertEquals(position_sizer.get_units(0.01, 5000, 200, price=1.7500), 1429)
       
       
       
       print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
    
    
    def test_ps_acct_denom_is_conv_base(self):
       position_sizer = PositionSizer("USDJPY", 10000, account_denomination="CHF")
       self.assertEquals(position_sizer.get_units(0.01, 5000, 100, price=85.00), 4250)
       
       
       
       position_sizer = PositionSizer("USDCAD", 10000, account_denomination="GBP")
       self.assertEquals(position_sizer.get_units(0.02, 10000, 200, price=1.74), 17400)
       print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
       
    def test_lots(self):
       position_sizer = PositionSizer("USDJPY", 100000, account_denomination="USD")
       units = position_sizer.get_units(0.02, 10000, 200, price=106.615)
       self.assertEquals(position_sizer.get_lots(units), 0.107)
       
       
       print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
        
if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "PositionSizerTest.test_ps_counter_currency" ).setLevel( logging.DEBUG )
    logging.getLogger( "PositionSizerTest.test_ps_base_currency" ).setLevel( logging.DEBUG )
    logging.getLogger( "PositionSizerTest.test_ps_acct_denom_is_conv_counter" ).setLevel( logging.DEBUG )
    logging.getLogger( "PositionSizerTest.test_ps_acct_denom_is_conv_base" ).setLevel( logging.DEBUG )

    unittest.main()