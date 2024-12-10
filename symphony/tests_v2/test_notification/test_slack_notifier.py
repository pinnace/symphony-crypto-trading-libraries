import unittest
import sys
import logging
from typing import List
from symphony.models import OrderModel
from symphony.enum import OrderStatus, AccountType, Exchange, Market, Timeframe
from symphony.data_classes import Order, Position
from symphony.config import USE_MODIN
from symphony.notification import SlackNotifier
from symphony.tests_v2.utils import dummy_instruments

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

instruments = dummy_instruments()
notifier_client = SlackNotifier(channel="notifier-test")

class SlackNotifierTest(unittest.TestCase):


    def test_notify_string(self):
        resp = notifier_client.notify_message("test")
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


    def test_notify_markdown_strings(self):
        strings = ["`test1`", ":poop:"]
        resp = notifier_client.notify_message(strings, is_markdown=True)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_notify_order(self):
        order = Order()
        order.order_id = 42069
        order.order_side = Market.BUY
        order.order_type = Market.MARKET
        order.price = 1.6969
        order.client_order_id = "ABC"
        order.timestamp = pd.Timestamp("2021-04-20 08:00:00")
        order.exchange = Exchange.BINANCE
        order.instrument = instruments[0]
        order.account = AccountType.ISOLATED_MARGIN
        order.commission_amount = 0.0420
        order.commission_asset = "BNB"
        order.quantity = 20
        order.stop_price = 1.420

        resp = notifier_client.notify_order(order, timeframe=Timeframe.H1)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_notify_position(self):
        position = Position()
        position.instrument = instruments[0]
        position.account_type = AccountType.ISOLATED_MARGIN
        position.position_id = "Setup|Buy|Test"
        position.side = Market.BUY
        position.borrow_amount = 200
        position.borrow_denomination = "USDT"
        position.margin_deposit = 0.005
        position.deposit_denomination = "ETH"
        position.margin_buy_amount = 300
        position.timeframe = Timeframe.H4
        position.entry_value = 600
        position.entry_denomination = "USDT"
        position.profit = 0.0
        resp = notifier_client.notify_position(position, status="OPENED")
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("SlackNotifierTest.test_notify_string").setLevel(logging.DEBUG)
    logging.getLogger("SlackNotifierTest.test_notify_markdown_strings").setLevel(logging.DEBUG)
    logging.getLogger("SlackNotifierTest.test_notify_order").setLevel(logging.DEBUG)
    logging.getLogger("SlackNotifierTest.test_notify_position").setLevel(logging.DEBUG)
    unittest.main()