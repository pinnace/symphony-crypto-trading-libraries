import unittest
import sys
import logging
from symphony.client import BinanceClient
from symphony.account import BinanceAccountManager
from symphony.quoter import BinanceRealTimeQuoter
from typing import List
from time import sleep

binance_client = BinanceClient()
quoter = BinanceRealTimeQuoter(binance_client)

class BinanceAccountManagerTest(unittest.TestCase):

    def test_account(self):
        bam = BinanceAccountManager(binance_client, isolated_margin_pairs="ADAEUR", historical_orders_mode=False)
        bam.create_isolated_margin_socket("AAVEBTC")

        total_balance = bam.total_free_account_value("EUR", quoter)
        self.assertGreater(total_balance, 0)
        bam.stop()
        quoter.stop()
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("BinanceAccountManagerTest.test_account").setLevel(logging.DEBUG)
    unittest.main()
