import unittest
import json
import os
import sys
import logging
from pprint import pprint
from os.path import join, dirname
import fastjsonschema
from symphony.schema import SchemaKit
from symphony.tests import TestingError


class SchemaTest(unittest.TestCase):
    def test_schema_kit(self):
        hit_method = False
        for schema_method in dir(SchemaKit):
            # Test all the getters in the SchemaKit class
            if not schema_method.startswith("__") and schema_method != 'schema' and schema_method.startswith("standard"):
                if not hit_method:
                    hit_method = True
                ph = getattr(SchemaKit,schema_method)()
                self.assertIsNotNone(ph)

        # If no methods were hit, consider this a fail
        if not hit_method:
            raise TestingError(__name__ + ": Did not hit any method")
        
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
        

        


if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "SchemaTest.test_schema" ).setLevel( logging.DEBUG )
    unittest.main()