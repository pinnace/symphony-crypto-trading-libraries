import unittest
import sys
import logging
from symphony.client import BinanceClient
from symphony.quoter import BinanceRealTimeQuoter
from time import sleep, perf_counter
from twisted.internet import reactor


class BinanceRealTimeQuoterTest(unittest.TestCase):

    def test_real_time_book(self):
        """
        binance_client = BinanceClient()
        quoter = BinanceRealTimeQuoter(binance_client)
        sleep(3)
        try:

            start_time: float = perf_counter()
            while not quoter.contains_all_instruments():
                sleep(1)
            end_time: float = perf_counter()
            print("Time to gather all symbols: {:10.4f}s".format(end_time - start_time))

            quoter.stop()
            #del quoter

        except KeyboardInterrupt:
            print("Closing web sockets...")
            del quoter
        """
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_get_bid_ask_liquidity_and_reconnect(self):
        binance_client = BinanceClient()
        quoter = BinanceRealTimeQuoter(binance_client)
        symbol = "ETHBTC"
        sleep(5)
        q1 = quoter.quotes[symbol].copy()
        bid = quoter.get_bid(symbol)
        ask = quoter.get_ask(symbol)
        bid_quantity = quoter.get_bid_quantity(symbol)
        ask_quantity = quoter.get_ask_quantity(symbol)
        midpoint = quoter.get_midpoint(symbol)
        liquidity = quoter.get_liquidity(symbol)
        print(f"Quote 1: {q1}")
        print(f"Live: Bid {symbol}: {bid}, Ask: {ask}")
        self.assertIsInstance(bid, float)
        self.assertIsInstance(ask, float)
        self.assertIsInstance(bid_quantity, float)
        self.assertIsInstance(ask_quantity, float)
        self.assertIsInstance(midpoint, float)
        self.assertGreater(midpoint, bid)
        self.assertLess(midpoint, ask)
        self.assertIsInstance(liquidity, float)
        self.assertGreater(liquidity, 0)

        sleep(5)
        q2 = quoter.quotes[symbol]
        bid2 = quoter.get_bid(symbol)
        ask2 = quoter.get_ask(symbol)
        bid_quantity2 = quoter.get_bid_quantity(symbol)
        ask_quantity2 = quoter.get_ask_quantity(symbol)
        midpoint2 = quoter.get_midpoint(symbol)
        liquidity2 = quoter.get_liquidity(symbol)
        print(f"Quote 2: {q2}")
        print(f"Bid2 {symbol}: {bid2}, Ask2: {ask2}")
        self.assertIsInstance(bid2, float)
        self.assertIsInstance(ask2, float)
        self.assertIsInstance(bid_quantity2, float)
        self.assertIsInstance(ask_quantity2, float)
        self.assertIsInstance(midpoint2, float)
        self.assertGreater(midpoint2, bid2)
        self.assertLess(midpoint2, ask2)
        self.assertIsInstance(liquidity, float)
        self.assertGreater(liquidity2, 0)

        self.assertNotEquals(q1, q2)
        quoter.stop()


        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("BinanceRealTimeQuoterTest.test_real_time_book").setLevel(logging.DEBUG)
    logging.getLogger("BinanceRealTimeQuoterTest.test_get_bid_ask").setLevel(logging.DEBUG)
    unittest.main()
