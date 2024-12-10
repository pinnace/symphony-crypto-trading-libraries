import unittest
import sys
import logging
from time import sleep
from symphony.client import BinanceClient
from symphony.enum import Timeframe, Exchange, Column
from symphony.data_classes import PriceHistory, Instrument, ConversionChain
from symphony.signal import DemarkSignal
from symphony.execution import BinanceTrader
from symphony.quoter import BinanceRealTimeQuoter
from symphony.account import BinanceAccountManager
from symphony.config import USE_MODIN
from symphony.utils.instruments import get_instrument
from twisted.internet import reactor
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


class DemarkSignalTest(unittest.TestCase):

    def test_signal(self):
        symbols_to_watch = []
        timeframes = Timeframe.H1
        binance_client = BinanceClient(websocket_symbols=symbols_to_watch, websocket_timeframes=timeframes)
        account_manager = BinanceAccountManager(binance_client, write_orders=False, create_isolated_margin_accounts=True)
        quoter = BinanceRealTimeQuoter(binance_client)
        trader = BinanceTrader(account_manager, quoter, allowed_spot_transfer_assets=["EUR", "USDT"])
        conv_chain = ConversionChain(quoter)

        for instrument in binance_client.get_all_instruments():
            if instrument.isolated_margin_allowed:
                if instrument.symbol not in symbols_to_watch:
                    symbols_to_watch.append(instrument.symbol)
        #symbols_to_watch = ["ZRXBTC"]
        signaler = DemarkSignal(trader, timeframes=timeframes, symbols_or_instruments=symbols_to_watch, margin=True, trade_signals=True)
        while 1:
            pass
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("DemarkSignalTest.test_signal").setLevel(logging.DEBUG)
    unittest.main()