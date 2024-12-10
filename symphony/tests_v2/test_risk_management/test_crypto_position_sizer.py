import unittest
import sys
import logging
from symphony.risk_management import CryptoPositionSizer
from symphony.quoter import BinanceRealTimeQuoter
from symphony.enum import Exchange, Market
from symphony.utils.instruments import get_instrument
from symphony.data_classes import Instrument, filter_instruments
from symphony.client import BinanceClient

account_balance = 10000
risk_perc = 0.02
amount_at_risk = account_balance * risk_perc
binance_client = BinanceClient()
quoter = BinanceRealTimeQuoter(binance_client)


class CryptoPositionSizerTest(unittest.TestCase):
    """
    def test_crypto_position_sizer_buy_quote_is_fiat(self):
        # Buying ADA
        target_symbols = ["ADABTC", "ETHEUR", "LINKETH", "BTCSTBTC"]
        base_assets = ["EUR", "BTC", "USDT"]

        for target_symbol in target_symbols:
            for base_asset in base_assets:
                target_instrument = filter_instruments(binance_client.get_all_symbols(), target_symbol)
                self.assertEquals(len(target_instrument), 1)
                target_instrument = target_instrument[0]

                entry_price = quoter.get_midpoint(target_symbol, fall_back_to_api=True)
                stop_loss = round(entry_price - (0.033 * entry_price), target_instrument.digits)
                order_type = Market.BUY

                pos_sizer = CryptoPositionSizer(quoter)

                position_size = pos_sizer.calculate_position_size(
                    base_asset,
                    target_instrument,
                    order_type,
                    entry_price,
                    stop_loss,
                    account_balance,
                    risk_perc,
                    fall_back_to_api=True
                )

                amount_at_risk = account_balance * risk_perc
                position_size_value_at_entry = position_size * entry_price
                base_asset_entry_value = pos_sizer.conversion_chain.convert(target_instrument.base_asset, base_asset,
                                                                            amount=position_size, order_type=order_type)
                position_size_value_after_loss = position_size * stop_loss
                stop_perc = (entry_price - stop_loss) / entry_price
                base_asset_stoploss_value = base_asset_entry_value * (1 - stop_perc)

                self.assertAlmostEquals(
                    base_asset_entry_value - base_asset_stoploss_value,
                    amount_at_risk,
                    places=0
                )

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_margin_requirement(self):

        instrument = get_instrument(binance_client.instruments, "ADAEUR")
        pos_sizer = CryptoPositionSizer(quoter)
        margin_ratio = 5
        margin_req = pos_sizer.required_margin_amount(instrument, 5.73, 0.2948, Market.BUY, margin_ratio)
        self.assertAlmostEquals(margin_req, 1.4329, 3)
        margin_req = pos_sizer.required_margin_amount(instrument, 567, 0.2648, Market.BUY, margin_ratio,
                                                      amount_in_terms_of_asset="EUR", position_size_denomination="ADA")
        curr_exchange_rate = pos_sizer.conversion_chain.convert("ADA", "EUR", amount=567)

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_smart_margin(self):
        instrument = get_instrument(binance_client.instruments, "ADAEUR")
        pos_sizer = CryptoPositionSizer(quoter)
        curr_price = quoter.get_midpoint(instrument, fall_back_to_api=True)
        sl_buy = curr_price - (curr_price * 0.05)
        sl_sell = curr_price + (curr_price * 0.05)
        margin_ratio = 5
        position_size = 5.73
        asset1, margin_req1 = pos_sizer.smart_margin_requirement(instrument, Market.BUY, margin_ratio, position_size, sl_buy)
        total_margin_available = (margin_req1 / curr_price) * (margin_ratio - 1)
        self.assertEquals(asset1, "EUR")
        self.assertGreater(total_margin_available, position_size)
        asset2, margin_req2 = pos_sizer.smart_margin_requirement(instrument, Market.SELL, margin_ratio, position_size, sl_sell)
        total_margin_available = (margin_req2 * curr_price) * (margin_ratio - 1)
        self.assertEquals(asset2, "ADA")
        self.assertGreater(total_margin_available, position_size)
        quoter.stop()
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")
    """
    def test_required_margin(self):
        instrument = get_instrument(binance_client.instruments, "ADAEUR")
        pos_sizer = CryptoPositionSizer(quoter)
        def verify_long(price, stop_loss, margin_required, pos_size):
            borrow = ((margin_required * (pos_size / margin_required)) - margin_required) * price
            borrow_buy = borrow / price
            initial_margin_level = ((margin_required + borrow_buy) * price) / borrow
            print(f"BUY: PS: {round(pos_size, 3)} {instrument.base_asset} Margin: {round(margin_required, 3)} {instrument.base_asset}, Initial ML: {round(initial_margin_level,  3)}, "
                  f"Borrow: {round(borrow, 3)} {instrument.quote_asset}, Margin Buy: {round(borrow_buy, 3)} {instrument.base_asset}")
            self.assertAlmostEqual(pos_size - borrow_buy, margin_required, 4)
            stop_loss_pos_val = (margin_required + borrow_buy) * stop_loss
            stop_loss_margin_level = stop_loss_pos_val / borrow

            self.assertAlmostEqual(stop_loss_margin_level, 1.3, 2)
            remaining = stop_loss_pos_val - borrow
            remaining_val = remaining
            loss = (margin_required * price) - remaining_val
            print(f"Stop Loss ML: {round(stop_loss_margin_level, 3)}, remaining: {round(remaining  / stop_loss, 3)} {instrument.base_asset}, "
                  f"Value {round(remaining_val, 3)} {instrument.quote_asset}, Loss: {round(loss, 3)} {instrument.quote_asset}")
            return loss

        price = 0.71988
        stop_loss = 0.6273
        pos_size = pos_sizer.calculate_position_size(instrument, Market.BUY, price, stop_loss, 10000, "EUR", 0.02, margin=True, fall_back_to_api=True)
        margin_asset, margin_required = pos_sizer.smart_margin(instrument, price, stop_loss, pos_size, Market.BUY)
        loss = verify_long(price, stop_loss, margin_required, pos_size)
        self.assertAlmostEqual((pos_size * price) - (pos_size * stop_loss), 200, 1)
        self.assertAlmostEqual(loss, 200, 1)

        def verify_short(price, stop_loss, margin_required, pos_size):
            pos_size *= price
            borrow = ((margin_required * (pos_size / margin_required)) - margin_required) / price
            borrow_buy = borrow * price
            initial_margin_level = ((margin_required + borrow_buy) / price) / borrow
            print(
                f"SELL: PS: {round(pos_size, 3)} {instrument.quote_asset}, Margin: {round(margin_required, 3)} {instrument.quote_asset}, Initial ML: {round(initial_margin_level, 3)}, "
                f"Borrow: {round(borrow, 3)} {instrument.base_asset}, Margin Buy: {round(borrow_buy, 3)} {instrument.quote_asset}")
            self.assertAlmostEqual(pos_size - borrow_buy, margin_required, 4)
            stop_loss_pos_val = (margin_required + borrow_buy) / stop_loss #ada
            stop_loss_margin_level = stop_loss_pos_val / borrow # ada
            self.assertAlmostEqual(stop_loss_margin_level, 1.3, 2)
            remaining = stop_loss_pos_val - borrow
            remaining_val = remaining * stop_loss
            loss = ((margin_required / price) - remaining ) * stop_loss
            print(f"Stop Loss ML: {round(stop_loss_margin_level, 3)}, remaining: {round(remaining  / stop_loss, 3)} {instrument.quote_asset}, "
                  f"Value {round(remaining_val, 3)} {instrument.quote_asset}, Loss: {round(loss, 3)} {instrument.quote_asset}")
            return loss

        price = 0.75562
        stop_loss = 0.80930
        pos_size = pos_sizer.calculate_position_size(instrument, Market.SELL, price, stop_loss, 10000, "EUR", 0.02, margin=True, fall_back_to_api=True)

        margin_asset, margin_required = pos_sizer.smart_margin(instrument, price, stop_loss, pos_size, Market.SELL)
        loss = verify_short(price, stop_loss, margin_required, pos_size)
        self.assertAlmostEqual((pos_size * stop_loss) - (pos_size * price), 200, 1)
        self.assertAlmostEqual(loss, 200, 1)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    quoter.stop()
    logging.getLogger("CryptoPositionSizerTest.test_crypto_position_sizer_buy_quote_is_fiat").setLevel(logging.DEBUG)
    logging.getLogger("CryptoPositionSizerTest.test_margin_requirement").setLevel(logging.DEBUG)
    unittest.main()
