from symphony.client import BinanceClient
from symphony.quoter import BinanceRealTimeQuoter
from symphony.account import BinanceAccountManager
from symphony.execution import BinanceTrader
from symphony.risk_management import CryptoPositionSizer
from symphony.utils.instruments import get_instrument
from symphony.config import ML_LOCAL_PATH
from symphony.enum import Timeframe, AccountType, BalanceType, Market
import pathlib
import pickle
from time import sleep

version = "latest-with-trend"
strategy = "DemarkBuySetup"
symbols_filename = "best_symbols"

def handler(event, context):
    symbols_path = pathlib.Path(ML_LOCAL_PATH) / f"{strategy}/" / f"{version}" / "saved_models" / f"{symbols_filename}.pkl"
    with open(symbols_path, "rb") as f:
        symbols = pickle.load(f)

    allowed_spot_assets = ["USDT", "BUSD", "EUR"]
    bc = BinanceClient()
    instrument = get_instrument(bc.instruments, "ADAEUR")

    quoter = BinanceRealTimeQuoter(bc)
    account_manager = BinanceAccountManager(bc, create_isolated_margin_accounts=True)

    trader = BinanceTrader(account_manager, quoter, allowed_spot_assets)
    pos_sizer = CryptoPositionSizer(quoter)

    price = quoter.get_price(instrument, Market.BUY, fall_back_to_api=True)
    stop_loss = round(price - (price * 0.05), instrument.digits)

    free_balance = account_manager.total_free_account_value("USDT", quoter, allowed_spot_assets=allowed_spot_assets)
    pos_size = pos_sizer.calculate_position_size(instrument, Market.BUY, price, stop_loss, free_balance, "USDT", 0.02,
                                                 margin=True, fall_back_to_api=True)
    margin_deposit_denom, margin_deposit = pos_sizer.smart_margin(instrument, price, stop_loss, pos_size, Market.BUY,
                                                                  instrument.isolated_margin_ratio)

    breakpoint()
    account_manager.create_isolated_margin_socket(instrument, create_isolated_margin_account=True)
    trader.enter_margin_position(AccountType.ISOLATED_MARGIN, "ADAEUR", pos_size, Market.BUY, margin_deposit, margin_deposit_denom, stop_loss)
    breakpoint()


if __name__ == "__main__":
    handler({"timeframe": "1h"}, {})