import unittest
import sys
import logging
from symphony.data_classes import ConversionChain, Instrument, filter_instruments, ConversionChainType
from symphony.client import BinanceClient
from symphony.quoter import BinanceRealTimeQuoter
from typing import List
from time import sleep

binance_client = BinanceClient()
instruments = binance_client.get_all_instruments()
binance_quoter = BinanceRealTimeQuoter(binance_client)
sleep(3)

class ConversionChainTest(unittest.TestCase):

    def test_conversion_chain(self):
        symbols = ["ETHBTC", "BTTTRX", "OAXBTC"]
        test_instruments = filter_instruments(instruments, symbols)
        start_assets = ["EUR", "BTC", "USDT"]
        cv = ConversionChain(binance_quoter, start_assets[0], test_instruments[0])

        for asset in start_assets:
            for instrument in test_instruments:
                cv.set_chain(instrument, start_asset=asset, highest_liquidity_chain=False)
                conversion_chain = cv.conversion_chain
                print(f"Conversion Chain: {conversion_chain}")
                instrument_chain = cv.instrument_chain
                execution_chain = cv.execution_chain

                cv.set_chain(instrument, start_asset=asset, highest_liquidity_chain=True)
                conversion_chain = cv.conversion_chain
                print(f"Conversion Chain: {conversion_chain}")
                instrument_chain = cv.instrument_chain
                execution_chain = cv.execution_chain


        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_conversion_chain_convert(self):
        starts = ["USDT", "BTC", "EUR", "OAX", "BTT"]
        ends = ["ADA", "ETH", "LINK", "BVND", "TRX"]
        instrument = filter_instruments(instruments, "ETHEUR")[0]
        cv = ConversionChain(binance_quoter, "EUR", instrument)
        for start in starts:
            for end in ends:
                cv.convert(start, end)

        for start in ends:
            for end in starts:
                cv.convert(start, end)

        amount = 1
        etheur_rate = binance_quoter.get_ask("ETHEUR", fall_back_to_api=True)
        conversion_rate = cv.convert("EUR", "ETH", amount=amount)
        self.assertAlmostEqual(amount / etheur_rate, conversion_rate, places=5)

        ethbtc_rate = binance_quoter.get_ask("ETHBTC", fall_back_to_api=True)
        conversion_rate = cv.convert("BTC", "ETH", amount=amount)
        self.assertAlmostEqual(amount / ethbtc_rate, conversion_rate, places=2)

        btceur_rate = binance_quoter.get_ask("BTCEUR", fall_back_to_api=True)
        trxbtc_rate = binance_quoter.get_ask("TRXBTC", fall_back_to_api=True)
        calc_rate = 1 / (btceur_rate * trxbtc_rate)
        conversion_rate = cv.convert("EUR", "TRX")
        self.assertAlmostEqual(calc_rate, conversion_rate, places=2)
        binance_quoter.stop()
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("ConversionChainTest.test_conversion_chain").setLevel(logging.DEBUG)
    unittest.main()
