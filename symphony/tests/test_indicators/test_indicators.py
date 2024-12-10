import unittest
import json
import os
import sys
import logging
from pprint import pprint
from symphony.indicators.indicator_registry import IndicatorRegistry
from symphony.indicators.indicators import Indicators
from symphony.tests.suite_exception import TestingError

class IndicatorsTest(unittest.TestCase):

    def test_indicator_registry_functions(self): 
        """
        Test if all indicators are matched to an enum. Cannot enumerate methods on Enum object.
        """
        enums = [indicator.name for indicator in list(IndicatorRegistry)]
        for enum in enums:
            if enum != "PRICE_HISTORY":
                try:
                    getattr(Indicators, enum.lower())
                except AttributeError:
                    raise TestingError(__name__ + ": Could not find an associated method for {}".format(enum))
                except Exception as e:
                    raise TestingError(__name__ + ": Unknown error: {}".format(str(e)))

        
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "IndicatorsTest.test_indicator_registry_functions" ).setLevel( logging.DEBUG )
    unittest.main()