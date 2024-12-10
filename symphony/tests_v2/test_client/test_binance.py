import unittest
import sys
import logging
from time import sleep
from symphony.client import BinanceClient
from symphony.enum import Timeframe, Exchange, Column
from symphony.data_classes import PriceHistory, Instrument
from symphony.config import USE_MODIN
from twisted.internet import reactor
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

binance_client = BinanceClient()

class BinanceClientTest(unittest.TestCase):

    """
    def test_binance_client_get_symbols(self):
        binance_client.get_all_symbols()
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_binance_client_get_instrument(self):
        instrument = Instrument(symbol="ETHBTC", digits=8, exchange=Exchange.BINANCE, is_currency=True)
        timeframe = Timeframe.M30
        num_bars = 100
        binance_ph: PriceHistory = binance_client.get(instrument, timeframe, num_bars)
        # Should be 100 bars
        self.assertEquals(len(binance_ph.price_history), num_bars)
        # Should be ETHBTC
        self.assertEquals(binance_ph.instrument.symbol, "ETHBTC")
        # Should be on Binance
        self.assertEquals(binance_ph.instrument.exchange, Exchange.BINANCE)
        # Should have 8 digits
        self.assertEquals(binance_ph.instrument.digits, 8)

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_binance_client_get_instrument_with_start_and_end(self):
        instrument = Instrument(symbol="ETHBTC", digits=8, exchange=Exchange.BINANCE, is_currency=True)
        timeframe = Timeframe.M5
        start_time = pd.Timestamp("2018-04-16 00:00:00", tz='UTC')
        end_time = pd.Timestamp("2018-06-16 00:00:00", tz='UTC')
        binance_ph: PriceHistory = binance_client.get(instrument, timeframe, start_time, end=end_time)
        self.assertEquals(start_time, binance_ph.price_history.index[0])
        self.assertEquals(end_time, binance_ph.price_history.index[-1])

    def test_binance_client_get_instrument_incomplete_bar(self):
        instrument = Instrument(symbol="ETHBTC", digits=8, exchange=Exchange.BINANCE, is_currency=True)
        timeframe = Timeframe.H1
        num_bars = 100
        binance_ph: PriceHistory = binance_client.get(instrument, timeframe, num_bars, incomplete_bar=True)
        # Should be 100 bars
        self.assertEquals(len(binance_ph.price_history), num_bars)
        # Should be ETHBTC
        self.assertEquals(binance_ph.instrument.symbol, "ETHBTC")
        # Should be on Binance
        self.assertEquals(binance_ph.instrument.exchange, Exchange.BINANCE)
        # Should have 8 digits
        self.assertEquals(binance_ph.instrument.digits, 8)
        close_price_1 = binance_ph.price_history[Column.CLOSE].iloc[-1]
        open_price_1 = binance_ph.price_history[Column.OPEN].iloc[-1]

        sleep(3)
        binance_ph: PriceHistory = binance_client.get(instrument, timeframe, num_bars, incomplete_bar=True)
        # Should be 100 bars
        self.assertEquals(len(binance_ph.price_history), num_bars)
        # Closes of most recent should be different
        self.assertNotEquals(close_price_1, binance_ph.price_history[Column.CLOSE].iloc[-1])
        # But Open should be the same
        self.assertEquals(open_price_1, binance_ph.price_history[Column.OPEN].iloc[-1])


        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_binance_websocket(self):
        binance_client_websocket = BinanceClient(websocket_symbols=["ADAEUR"], websocket_timeframes=[Timeframe.H4])
        sleep(3)
        self.assertIsNotNone(binance_client_websocket.price_histories["ADAEUR"][Timeframe.H4])
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


    def test_binance_websocket_many_and_incomplete(self):
        binance_client_websocket = BinanceClient(websocket_symbols=["EURUSDT"], websocket_timeframes=Timeframe.H1)
        binance_client_websocket.start_candle_websocket("ADAEUR", Timeframe.H4, incomplete_bars=True)
        binance_client_websocket.start_candle_websocket("ETHBTC", Timeframe.H4, incomplete_bars=True)
        sleep(8)
        self.assertIsNotNone(binance_client_websocket.price_histories["ADAEUR"][Timeframe.H4])
        breakpoint()
        timestamp1 = binance_client_websocket.price_histories["ADAEUR"][Timeframe.H4].price_history
        binance_client_websocket.stop()
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


    def test_binance_websocket_after_init(self):
        binance_client_websocket = BinanceClient()
        sleep(5)
        binance_client_websocket.start_candle_websocket("ADAEUR", Timeframe.H4, incomplete_bars=True)
        sleep(10)
        self.assertIsNotNone(binance_client_websocket.price_histories["ADAEUR"][Timeframe.H4])
        index1 = binance_client_websocket.price_histories["ADAEUR"][Timeframe.H4].price_history.index[-1]
        bar1 = binance_client_websocket.price_histories["ADAEUR"][Timeframe.H4].price_history.iloc[-1]
        sleep(3)
        index2 = binance_client_websocket.price_histories["ADAEUR"][Timeframe.H4].price_history.index[-1]
        bar2 = binance_client_websocket.price_histories["ADAEUR"][Timeframe.H4].price_history.iloc[-1]
        self.assertEquals(index1, index2)
        self.assertNotEqual(bar1["close"], bar2["close"])
        binance_client_websocket.stop()
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")
    
    def test_binance_websocket_after_init_no_incomplete(self):
        binance_client_websocket = BinanceClient()
        binance_client_websocket.start_candle_websocket("ADAEUR", Timeframe.M1)

        self.assertIsNotNone(binance_client_websocket.price_histories["ADAEUR"][Timeframe.M1])
        index1 = binance_client_websocket.price_histories["ADAEUR"][Timeframe.M1].price_history.index[-1]
        bar1 = binance_client_websocket.price_histories["ADAEUR"][Timeframe.M1].price_history.iloc[-1]
        len1 = len(binance_client_websocket.price_histories["ADAEUR"][Timeframe.M1].price_history)
        sleep(60)
        index2 = binance_client_websocket.price_histories["ADAEUR"][Timeframe.M1].price_history.index[-1]
        bar2 = binance_client_websocket.price_histories["ADAEUR"][Timeframe.M1].price_history.iloc[-1]
        len2 = len(binance_client_websocket.price_histories["ADAEUR"][Timeframe.M1].price_history)
        self.assertNotEquals(index1, index2)
        self.assertNotEqual(bar1["close"], bar2["close"])
        self.assertNotEqual(bar1["open"], bar2["open"])
        self.assertNotEqual(bar1["high"], bar2["high"])
        self.assertNotEqual(bar1["low"], bar2["low"])
        self.assertNotEqual(bar1["volume"], bar2["volume"])
        self.assertEquals(index2, index1 + pd.Timedelta(minutes=1))
        self.assertNotEqual(len1, len2)
        binance_client_websocket.stop()
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")
    """
    def test_binance_anon_get(self):
        instrument = Instrument(symbol="ETHBTC", base_asset="ETH", quote_asset="BTC", digits=8, exchange=Exchange.BINANCE, is_currency=True)

        timeframe = Timeframe.H1
        num_bars = 300
        ph = BinanceClient.anon_get(instrument, timeframe, num_bars_or_start_time=num_bars)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("BinanceClientTest.test_binance_client_get_symbols").setLevel(logging.DEBUG)
    logging.getLogger("BinanceClientTest.test_binance_client_get_instrument").setLevel(logging.DEBUG)
    logging.getLogger("BinanceClientTest.test_binance_client_get_instrument_with_start_and_end").setLevel(logging.DEBUG)
    logging.getLogger("BinanceClientTest.test_binance_client_get_instrument_incomplete_bar").setLevel(logging.DEBUG)
    logging.getLogger("BinanceClientTest.test_binance_websocket").setLevel(logging.DEBUG)
    logging.getLogger("BinanceClientTest.test_binance_websocket_incomplete").setLevel(logging.DEBUG)
    unittest.main()
