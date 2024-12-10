
import unittest
import json
import os
import sys
import logging
from pprint import pprint
from os.path import join, dirname
from symphony.risk_management.historical_rate_converter import HistoricalRatesConverter


class HistoricalRangeConverterTest(unittest.TestCase):
    
    def test_hrc_counter_currency(self):
       hrc = HistoricalRatesConverter('Oanda', 'H1', "EURUSD", 10000)
       self.assertEquals(hrc.get_units(0.01, 5000, 200, '2013-01-02 14:00:00'), 2500)
       print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
       
    def test_hrc_base_currency(self):
       hrc = HistoricalRatesConverter('Oanda', 'H1', "USDJPY", 10000)
       print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
       
    
    def test_hrc_acct_denom_is_conv_counter(self):
       hrc = HistoricalRatesConverter('Oanda', 'H1', "EURGBP", 10000)
       self.assertEquals(hrc.get_units(0.02, 10000, 200, '2013-01-08 00:00:00'), 6206)
       print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
       
    def test_hrc_acct_denom_is_conv_base(self):
       hrc = HistoricalRatesConverter('Oanda', 'H1', "EURJPY", 10000)
       self.assertEquals(hrc.get_units(0.015, 78239, 234, '2013-01-07 23:00:00'), 43768)
       print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
       
    def test_hrc_lots(self):
       hrc = HistoricalRatesConverter('Oanda', 'H1', "EURJPY", 100000)
       units = hrc.get_units(0.015, 78239, 234, '2013-01-07 23:00:00')
       self.assertEquals(hrc.get_lots(units), 0.438)
       
       
       print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
        
if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "HistoricalRangeConverterTest.test_hrc_counter_currency" ).setLevel( logging.DEBUG )
    logging.getLogger( "HistoricalRangeConverterTest.test_hrc_base_currency" ).setLevel( logging.DEBUG )
    logging.getLogger( "HistoricalRangeConverterTest.test_hrc_acct_denom_is_conv_counter" ).setLevel( logging.DEBUG )
    logging.getLogger( "HistoricalRangeConverterTest.test_hrc_acct_denom_is_conv_base" ).setLevel( logging.DEBUG )
    logging.getLogger( "HistoricalRangeConverterTest.test_hrc_lots" ).setLevel( logging.DEBUG )

    unittest.main()