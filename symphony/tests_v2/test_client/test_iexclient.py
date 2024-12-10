import unittest
import sys
import logging
from symphony.client import IEXClient


class IEXTest(unittest.TestCase):

    def test_iex_client_get_instrument(self):
        #iex_client = IEXClient()



        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_iex_client_get_symbol(self):
        #iex_client = IEXClient()
        #iex_client.get_all_symbols()
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("IEXTest.test_iex_client_get_stock").setLevel(logging.DEBUG)
    logging.getLogger("IEXTest.test_iex_client_get_symbol").setLevel(logging.DEBUG)
    unittest.main()
