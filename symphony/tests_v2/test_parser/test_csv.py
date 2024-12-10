import unittest
import sys
import logging
from os.path import dirname
from symphony.parser import CSVParser
from symphony.exceptions import ParserClassException


class CSVParserTest(unittest.TestCase):
    csv_file = dirname(dirname(__file__)) + "/data/test_data.csv"

    def test_csv_parser(self):
        csv_parser = CSVParser()

        columns = ['low', 'close', 'high', 'volume', 'timestamp', 'open']
        candles = csv_parser.parse(self.csv_file, datetime_column='timestamp')

        self.assertEquals(candles["volume"][0], 6611.0)
        self.assertEquals(candles["open"][0], 1.10913)
        self.assertEquals(candles["close"].iloc[-1], 1.10181)

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("CSVParserTest.test_csv_parser").setLevel(logging.DEBUG)
    unittest.main()
