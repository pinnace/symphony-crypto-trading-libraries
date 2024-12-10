import unittest
import sys
import os
import logging
from symphony.data.archivers import BinanceArchiver
from symphony.config import TRADING_LIB_DIR, config, USE_S3
from symphony.backtest.results import ResultsHelper
from symphony.data_classes import Instrument
from symphony.enum import Timeframe
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)


class BinanceArchiverTest(unittest.TestCase):

    """
    def test_binance_archiver_save(self):
        instrument = Instrument()
        instrument.symbol = "ETHBTC"
        timeframe = Timeframe.H4
        archiver = BinanceArchiver(TRADING_LIB_DIR + "data/")
        archiver.save(instrument, timeframe)
        save_path = archiver.__get_save_path(instrument, timeframe)
        os.remove(save_path)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_binance_archiver_update(self):
        instrument = Instrument()
        instrument.symbol = "ETHBTC"
        timeframe = Timeframe.H4
        archiver = BinanceArchiver(TRADING_LIB_DIR + "data/")
        archiver.save(instrument, timeframe)
        archiver.update(instrument, timeframe)
        save_path = archiver.__get_save_path(instrument, timeframe)
        os.remove(save_path)
        #archiver.save_and_update(instrument, timeframe)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_binance_archiver_save_multiple(self):
        instrument = Instrument()
        instrument.symbol = "ETHBTC"
        timeframe = Timeframe.H4
        archiver = BinanceArchiver(TRADING_LIB_DIR + "data/")
        #archiver.save_multiple()
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_binance_archiver_save_s3(self):
        instrument = Instrument()
        instrument.symbol = "ETHBTC"
        timeframe = Timeframe.H4
        if USE_S3:
            archiver = BinanceArchiver(config["archive"]["s3_bucket"], use_s3=USE_S3)
            archiver.save(instrument, timeframe)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_binance_archiver_update_s3(self):
        instrument = Instrument()
        instrument.symbol = "ETHBTC"
        timeframe = Timeframe.H4
        if USE_S3:
            archiver = BinanceArchiver(config["archive"]["s3_bucket"], use_s3=USE_S3)
            archiver.update(instrument, timeframe)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")
    """
    def test_binance_archiver_save_update_multiple_s3(self):
        rh = ResultsHelper("Demark", use_s3=False)
        instrument = Instrument()
        instrument.symbol = "ETHBTC"
        if USE_S3:
            archiver = BinanceArchiver(config["archive"]["s3_bucket"], use_s3=USE_S3)
            archiver.save_and_update_multiple(instruments=[instrument for instrument in rh.instruments if instrument.quote_asset in ["USDT", "BUSD", "EUR", "USDC"]], timeframes=[Timeframe.H1, Timeframe.H4])
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    """
    def test_archiver_scan(self):
        rh = ResultsHelper("Demark", use_s3=False)
        instrument = [instrument for instrument in rh.instruments if instrument.symbol == "ATOMUSDT"][0]
        if USE_S3:
            archiver = BinanceArchiver(config["archive"]["s3_bucket"], use_s3=USE_S3)
            archiver.scan(instrument, Timeframe.H1, verbose=True)

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")
    """
if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("BinanceArchiverTest.test_binance_archiver").setLevel(logging.DEBUG)
    logging.getLogger("BinanceArchiverTest.test_binance_archiver_save_s3").setLevel(logging.DEBUG)
    logging.getLogger("BinanceArchiverTest.test_binance_archiver_update_s3").setLevel(logging.DEBUG)
    logging.getLogger("BinanceArchiverTest.test_binance_archiver_save_update_multiple_s3").setLevel(logging.DEBUG)
    unittest.main()
