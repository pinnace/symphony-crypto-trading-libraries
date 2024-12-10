import unittest
import sys
import logging


class AlphaVantageTest(unittest.TestCase):

    def test_alphavantage_client(self):


        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("AlphaVantageTest.test_alphavantage_client").setLevel(logging.DEBUG)
    unittest.main()
