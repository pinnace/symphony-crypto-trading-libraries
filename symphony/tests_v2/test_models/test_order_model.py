import unittest
import sys
import logging
from typing import List
from symphony.models import OrderModel
from symphony.enum import OrderStatus, AccountType, Exchange, Market
from symphony.config import USE_MODIN

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


class OrderModelTest(unittest.TestCase):

    def test_new_order(self):
        OrderModel.Meta.table_name = "OrdersTest"
        if not OrderModel.exists():
            OrderModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)


        new_order = OrderModel()
        new_order.symbol = "ETHBTC"
        new_order.account_type = AccountType.SPOT
        new_order.base_asset = "ETH"
        new_order.quote_asset = "BTC"
        new_order.digits = 5
        new_order.order_id = 1234567
        new_order.client_order_id = "420"
        new_order.order_status = OrderStatus.FILLED
        new_order.exchange = Exchange.BINANCE.name
        new_order.commission_amount = 0.00032
        new_order.commission_asset = "BNB"
        new_order.price = 0.044
        new_order.stop_price = 0.034
        new_order.quantity = 1.69
        new_order.filled_quantity = 1.69
        new_order.order_side = Market.BUY
        new_order.order_type = Market.MARKET
        new_order.order_placed_time = pd.Timestamp(1513393355.5, unit='s', tz='UTC')
        new_order.order_last_trade_time = pd.Timestamp(1513393355.5, unit='s', tz='UTC')
        new_order.save()

        self.assertEquals(OrderModel.count(1234567, OrderModel.symbol == 'ETHBTC'), 1)
        self.assertEquals(OrderModel.count(1234567, OrderModel.order_status == OrderStatus.FILLED), 1)
        self.assertEquals(OrderModel.describe_table()['TableName'], 'OrdersTest')

        for order in OrderModel.scan(OrderModel.symbol == 'ETHBTC'):
            self.assertEquals(order.order_status, OrderStatus.FILLED)
            self.assertGreater(order.order_placed_time, pd.Timestamp(1513383355.5, unit='s', tz='UTC'))
        for order in OrderModel.scan(OrderModel.order_placed_time >= pd.Timestamp(1513383355.5, unit='s', tz='UTC')):
            self.assertEquals(order.order_status, OrderStatus.FILLED)

        # Test update order
        new_order = OrderModel()
        new_order.symbol = "ETHBTC"
        new_order.account_type = AccountType.SPOT
        new_order.base_asset = "ETH"
        new_order.quote_asset = "BTC"
        new_order.digits = 5
        new_order.order_id = 1234567
        new_order.client_order_id = "420"
        new_order.order_status = OrderStatus.CANCELLED
        new_order.exchange = Exchange.BINANCE.name
        new_order.commission_amount = 0.00032
        new_order.commission_asset = "BNB"
        new_order.price = 0.044
        new_order.stop_price = 0.034
        new_order.quantity = 1.69
        new_order.filled_quantity = 0.0
        new_order.order_side = Market.BUY
        new_order.order_type = Market.MARKET
        new_order.order_placed_time = pd.Timestamp(1513393355.5, unit='s', tz='UTC')
        new_order.order_last_trade_time = pd.Timestamp(1513393355.5, unit='s', tz='UTC')
        new_order.save()

        orders = [order for order in OrderModel.scan(OrderModel.symbol == 'ETHBTC')]
        self.assertEquals(len(orders), 1)
        self.assertEquals(orders[0].order_status, OrderStatus.CANCELLED)

        OrderModel.delete_table()
        self.assertFalse(OrderModel.exists())



        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")



if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("OrderModelTest.test_order").setLevel(logging.DEBUG)
    unittest.main()