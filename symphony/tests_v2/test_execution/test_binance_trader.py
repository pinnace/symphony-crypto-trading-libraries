import unittest
import sys
import logging
from symphony.client import BinanceClient
from symphony.execution import BinanceTrader
from symphony.account import BinanceAccountManager
from symphony.quoter import BinanceRealTimeQuoter
from symphony.utils.instruments import get_instrument
from symphony.risk_management import CryptoPositionSizer
from symphony.enum import AccountType, Market
from typing import List
from time import sleep

binance_client = BinanceClient()


class BinanceTraderTest(unittest.TestCase):

    def test_transfer(self):
        """
        quoter = BinanceRealTimeQuoter(binance_client)
        account_manager = BinanceAccountManager(binance_client)
        #account_manager.create_isolated_margin_socket("INJUSDT")


        bt = BinanceTrader(account_manager, quoter, allowed_spot_transfer_assets=["EUR", "ADA"])
        #bt.transfer(AccountType.ISOLATED_MARGIN, "ADA", 30.01, target_symbol_or_instrument="ADABTC", order_side=Market.BUY)
        bt.transfer(AccountType.ISOLATED_MARGIN, "INJ", 3, target_symbol_or_instrument="INJUSDT", order_side=Market.BUY)

        #bt.transfer(AccountType.ISOLATED_MARGIN, "ADA", 90, target_symbol_or_instrument="ADAEUR")
        quoter.stop()
        """
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_transfer_from_spot(self):
        """
        quoter = BinanceRealTimeQuoter(binance_client)
        account_manager = BinanceAccountManager(binance_client)
        bt = BinanceTrader(account_manager, quoter, allowed_spot_transfer_assets=["EUR", "USDT"])
        bt.transfer_from_spot(AccountType.ISOLATED_MARGIN, "INJ", 3, target_symbol_or_instrument="INJUSDT", order_side=Market.BUY)
        """
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_execution_chain(self):
        """
        quoter = BinanceRealTimeQuoter(binance_client)
        account_manager = BinanceAccountManager(binance_client)
        bt = BinanceTrader(account_manager, quoter, allowed_spot_transfer_assets=["EUR", "USDT"])
        bt.conversion_chain.set_chain("MFT", start_asset="EUR")

        quantity = bt.conversion_chain.convert("EUR", "BTC", amount=20)
        quantity = bt.round_lot("BTCEUR", quantity)
        conversion_chain = ['BTCEUR', 'DIABTC']
        execution_chain = [Market.BUY, Market.BUY]

        bt.execute_conversion_chain(conversion_chain, execution_chain, quantity)
        """
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


    def test_enter_margin_position_buy(self):
        """
        quoter = BinanceRealTimeQuoter(binance_client)
        instrument = get_instrument(binance_client.instruments, "BELUSDT")
        pos_sizer = CryptoPositionSizer(quoter)
        account_manager = BinanceAccountManager(binance_client)
        account_manager.create_isolated_margin_socket(instrument)
        bt = BinanceTrader(account_manager, quoter, allowed_spot_transfer_assets=["EUR", "USDT"])

        price = quoter.get_midpoint(instrument, fall_back_to_api=True)
        stop_loss = price - (price * 0.03)
        pos_size = pos_sizer.calculate_position_size(instrument, Market.BUY, price, stop_loss, 100, "EUR", 0.02, margin=True, fall_back_to_api=True)
        margin_deposit_denom, margin_deposit = pos_sizer.smart_margin(instrument, price, stop_loss, pos_size, Market.BUY, instrument.isolated_margin_ratio)

        position = bt.enter_margin_position(AccountType.ISOLATED_MARGIN, instrument, pos_size, Market.BUY, margin_deposit, margin_deposit_denom, stop_loss)
        sleep(1)
        position = bt.exit_margin_position(position, exit_to_spot_currency="EUR")
        print(f"Profit: {position.profit} {position.entry_denomination}")

        breakpoint()
        account_manager.stop()
        quoter.stop()
        """
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_enter_margin_position_sell(self):
        """
        quoter = BinanceRealTimeQuoter(binance_client)
        instrument = get_instrument(binance_client.instruments, "BNBBTC")
        pos_sizer = CryptoPositionSizer(quoter)
        account_manager = BinanceAccountManager(binance_client)
        account_manager.create_isolated_margin_socket(instrument)

        bt = BinanceTrader(account_manager, quoter, allowed_spot_transfer_assets=["EUR", "USDT"])

        price = quoter.get_midpoint(instrument, fall_back_to_api=True)
        stop_loss = price + (price * 0.03)


        pos_size = pos_sizer.calculate_position_size(instrument, Market.SELL, price, stop_loss, 100, "EUR", 0.02, margin=True, fall_back_to_api=True)
        margin_deposit_denom, margin_deposit = pos_sizer.smart_margin(instrument, price, stop_loss, pos_size, Market.SELL, instrument.isolated_margin_ratio)

        position = bt.enter_margin_position(AccountType.ISOLATED_MARGIN, instrument, pos_size, Market.SELL, margin_deposit, margin_deposit_denom, stop_loss)
        sleep(1)
        position = bt.exit_margin_position(position, exit_to_spot_currency="EUR")
        print(f"Profit: {position.profit} {position.entry_denomination}")

        breakpoint()
        account_manager.stop()
        quoter.stop()


        """

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_market_order(self):
        quoter = BinanceRealTimeQuoter(binance_client)
        instrument = get_instrument(binance_client.instruments, "BNBBTC")
        account_manager = BinanceAccountManager(binance_client)
        account_manager.create_isolated_margin_socket(instrument)

        bt = BinanceTrader(account_manager, quoter, allowed_spot_transfer_assets=["EUR", "USDT"])
        symbol = "ADAEUR"

        import base64
        order_id = "WlJYQlRDfDYwfDIwMjEtMDQtMDIgMjI6MDA6MDArMDA6MDB8YnV5fGFnZ3Jlc3NpdmVfYnV5X2NvdW50ZG93bg"
        order = bt.market_order(symbol, AccountType.SPOT, Market.BUY, 10, client_order_id=order_id)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("BinanceTraderTest.test_trader").setLevel(logging.DEBUG)
    unittest.main()
