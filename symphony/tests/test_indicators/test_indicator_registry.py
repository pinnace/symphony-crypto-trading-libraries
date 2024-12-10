import unittest
import json
import os
import sys
import logging
from pprint import pprint
from symphony.indicators.indicator_registry import IndicatorRegistry
from symphony.tests.suite_exception import TestingError



class IndicatorRegistryTest(unittest.TestCase):
    def test_indicator_registry_enum(self):
        """
        Test if all enums match expected values
        """
        for i, indicator in enumerate(IndicatorRegistry):
            self.assertEqual(indicator, i)
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")

    


if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "IndicatorRegistryTest.test_indicator_registry_enum" ).setLevel( logging.DEBUG )
    unittest.main()