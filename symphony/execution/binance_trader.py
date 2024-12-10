from symphony.abc import ExchangeTraderABC
from symphony.account import BinanceAccountManager
from symphony.client import BinanceClient
from symphony.quoter import BinanceRealTimeQuoter
from symphony.risk_management import CryptoPositionSizer
from symphony.enum import Market, AccountType, BalanceType, OrderStatus, Exchange
from symphony.data_classes import Instrument, ConversionChain, Order, Signal, Position
from symphony.utils.graph import ConversionChainType
from symphony.exceptions import ExecutionException
from symphony.utils.instruments import get_instrument
from symphony.utils.orders import order_from_cctx
from symphony.config import LOG_LEVEL
from binance.exceptions import BinanceAPIException
from binance.enums import SIDE_BUY, SIDE_SELL, TIME_IN_FORCE_GTC
from typing import List, Optional, Union, Dict, Callable, Tuple
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import concurrent.futures
from time import sleep
from concurrent.futures._base import ALL_COMPLETED
import shortuuid
import logging
import time

logger = logging.getLogger(__name__)


class BinanceTrader(ExchangeTraderABC):
    """
    BinanceTrader

        Handles trading and transferring. Provides standard trading functions, plus a few others
        to make lives a little easier.
        Advanced functions include:
            - transfer() -> Transfer or trade way into required amount for arbitrary account
            - Various additional transfer functions. Transparently transfer from isolated margin to isolated margin without having to mess with bouncing through spot
            - Listens for orders and balance changes on WebSockets, if function returns successfully, then the order or transfer happened
    """

    def __init__(self,
                 account_manager: BinanceAccountManager,
                 quoter: Union[BinanceRealTimeQuoter],
                 allowed_spot_transfer_assets: List[str],
                 log_level: Optional[int] = LOG_LEVEL):
        """
        Instantiates BinanceTrader

        :param account_manager: An instance of BinanceAccountManager
        :param quoter: Instance of BinanceRealTimeQuoter
        :param allowed_spot_transfer_assets: Whitelist of allowed spot transfer assets. Required.
        :param log_level: Optional Log level
        """
        super().__init__()
        if not isinstance(account_manager, BinanceAccountManager):
            raise ExecutionException(f"{account_manager} is not a BinanceAccountManager!")
        if not isinstance(quoter, BinanceRealTimeQuoter):
            raise ExecutionException(f"{quoter} is not a BinanceRealTimeQuoter!")
        self.account_manager: BinanceAccountManager = account_manager
        self.quoter: BinanceRealTimeQuoter = quoter
        self.client: BinanceClient = quoter.symphony_client
        self.binance_client = self.client.binance_client
        self.ccxt_client = self.client.ccxt_client
        self.position_sizer = CryptoPositionSizer(self.quoter, log_level=log_level)
        self.conversion_chain: ConversionChain = ConversionChain(self.quoter)
        self.instruments: List[Instrument] = self.client.instruments
        self.__isolated_margin_pairs: List[str] = [instrument.symbol for instrument in self.instruments if
                                                   instrument.isolated_margin_allowed]
        self.__allowed_spot_transfer_assets: List[str] = allowed_spot_transfer_assets

        for asset in allowed_spot_transfer_assets:
            if asset not in self.client.get_all_assets():
                raise ExecutionException(f"{asset} is unknown asset")

        logger.setLevel(log_level)
        return

    @property
    def allowed_spot_assets(self) -> List[str]:
        return self.__allowed_spot_transfer_assets

    def market_order(self,
                     symbol_or_instrument: Union[str, Instrument],
                     account_type: AccountType,
                     order_side: Market,
                     quantity: float,
                     stop_loss: Optional[float] = 0.0,
                     take_profit: Optional[float] = -1,
                     client_order_id: Optional[str] = "",
                     websocket_keepalive: Optional[bool] = True,
                     auto_repay_margin: Optional[bool] = True
                     ) -> Order:
        """
        Performs market order for given side

        :param symbol_or_instrument: Symbol to trade
        :param account_type: Account to trade from
        :param order_side: Market.BUY or Market.SELL
        :param quantity: Quantity. Must be rounded to lot.
        :param stop_loss: Optionally create STOP_LOSS_LIMIT
        :param take_profit: Create an OCO order in conjunction with stop loss
        :param client_order_id: Optional user specified order identifier
        :param websocket_keepalive: If trading isolated margin, keep the socket open for performance
        :param auto_repay_margin: Set side effect to AUTO_REPAY for orders in accounts with open margin positions
        :return: The completed Order object
        :raises ExecutionException: Invalid account, invalid order side, invalid quantity
        """
        if not isinstance(account_type, AccountType):
            raise ExecutionException(f"Invalid account type: {account_type}")
        if order_side not in [Market.BUY, Market.SELL]:
            raise ExecutionException(f"Invalid order side: {order_side}")
        if quantity <= 0:
            raise ExecutionException(f"Invalid quantity: {quantity}")
        instrument = get_instrument(self.instruments, symbol_or_instrument)
        if self.round_lot(instrument, quantity) != quantity:
            raise ExecutionException(f"Quantity must be rounded to correct step size")

        ccxt_symbol = instrument.base_asset + "/" + instrument.quote_asset

        transfer_uuid = self.__new_transfer_global()

        if account_type == AccountType.ISOLATED_MARGIN:
            self.account_manager.create_isolated_margin_socket(instrument.symbol)

        order_handler = self.__order_listener_template(transfer_uuid, account_type, quantity, instrument.symbol)
        self.account_manager.register_order_callback(order_handler)

        logger.info(f"Executing MARKET order {account_type} / {instrument.symbol} / {order_side} / {quantity} / {client_order_id if client_order_id else 'NO_ID'}")
        params = self.__ccxt_params(account_type, client_order_id=client_order_id, auto_repay=auto_repay_margin)

        try:
            self.ccxt_client.create_order(ccxt_symbol, 'market', order_side.value, quantity, params=params)
        except Exception as e:
            err = str(e)
            breakpoint()
            raise ExecutionException(f"Caught exception: {str(e)}\n"
                                     f"Symbol: {ccxt_symbol}, side {order_side}, Q: {quantity}, params: {params}")

        try:
            self.__wait_for_order(transfer_uuid, Market.MARKET, order_side, instrument.symbol, quantity, account_type)
            order = globals()[transfer_uuid + "_order"]
            del globals()[transfer_uuid]
            del globals()[transfer_uuid + "_order"]
        except ExecutionException:
            logger.debug(f"Wait for {instrument.symbol} / {order_side} / {quantity} failed, trying API ")
            order = self.account_manager.get_order_by_quantity(account_type, instrument, order_side, quantity)
            if not order:
                raise ExecutionException(f"Could not detect successful execution for "
                                         f"market order for {instrument.symbol} / {order_side} / {quantity} failed.")

        if take_profit > 0.0:
            if not stop_loss:
                raise ExecutionException(f"A Stop loss must be specified alongside a take profit level")

        if take_profit == -1 and stop_loss:
            self.stop_loss_for_market(instrument, account_type, order_side, quantity, stop_loss)

        self.account_manager.deregister_order_callback(order_handler)

        if account_type == AccountType.ISOLATED_MARGIN and not websocket_keepalive:
            self.account_manager.stop_isolated_margin_socket(instrument.symbol)
        return order

    def oco_order(self, symbol_or_instrument: Union[str, Instrument], order_side: Market, account_type: AccountType, price: float, stop: float, limit: float, quantity: float):
        instrument = get_instrument(self.instruments, symbol_or_instrument)
        if self.round_lot(instrument, quantity) != quantity:
            raise ExecutionException(f"Quantity must be rounded to correct step size")

        bc_side = SIDE_SELL if order_side == Market.BUY else SIDE_BUY

        resp = self.client.binance_client.create_oco_order(
            symbol=instrument.symbol,
            side=bc_side,
            stopLimitTimeInForce=TIME_IN_FORCE_GTC,
            quantity=quantity,
            stopPrice=str(stop),
            price=str(price)
        )
        breakpoint()



    def stop_loss_for_market(self,
                             symbol_or_instrument: Union[str, Instrument],
                             account_type: AccountType,
                             order_side: Market,
                             quantity: float,
                             stop_loss: float
                             ) -> Order:

        """
        Places a STOP_LOSS_LIMIT order

        :param symbol_or_instrument: Instrument or symbol
        :param account_type: Account
        :param order_side: BUY or SELL
        :param quantity: Quantity
        :param stop_loss: Stop loss
        :return: Order object
        """
        if order_side not in [Market.BUY, Market.SELL]:
            raise ExecutionException(f"{order_side} must be BUY or SELL")

        instrument = get_instrument(self.instruments, symbol_or_instrument)
        ccxt_symbol = instrument.base_asset + "/" + instrument.quote_asset
        params = self.__ccxt_params(account_type, auto_repay=True)

        stop_loss = round(stop_loss, instrument.digits)
        if order_side == Market.BUY:
            params['stopPrice'] = str(stop_loss + (0.001 * stop_loss))
            resp = self.ccxt_client.create_order(ccxt_symbol, 'STOP_LOSS_LIMIT', side=Market.SELL.value, price=stop_loss, amount=quantity, params=params)
            return order_from_cctx(resp, Exchange.BINANCE, instrument, account_type)
        else:
            params['stopPrice'] = str(stop_loss - (0.001 * stop_loss))
            resp = self.ccxt_client.create_order(ccxt_symbol, 'STOP_LOSS_LIMIT', side=Market.BUY.value, price=stop_loss, amount=quantity, params=params)
            return order_from_cctx(resp, Exchange.BINANCE, instrument, account_type)

    def round_lot(self, symbol_or_instrument: Union[Instrument], quantity: float, strategy: Optional[str] = ROUND_DOWN) -> float:
        """
        Rounds an order quantity to the instrument's step size

        :param symbol_or_instrument: Symbol or instrument
        :param quantity: Order quantity
        :param strategy: either decimal.ROUND_DOWN or decimal.ROUND_UP
        :return: Normalized lot size
        :raises ExecutionException: If rounding strategy unknown
        """
        if strategy != ROUND_UP and strategy != ROUND_DOWN:
            raise ExecutionException(f"Unknown rounding strategy: {strategy}")
        instrument = get_instrument(self.instruments, symbol_or_instrument)
        # return float(Decimal(str(quantity)).quantize(Decimal(str(instrument.step_size)), strategy))
        ccxt_symbol = instrument.base_asset + "/" + instrument.quote_asset
        return float(self.ccxt_client.amount_to_precision(ccxt_symbol, quantity))

    def transfer_from_spot(self,
                           target_account: AccountType,
                           target_asset: str,
                           target_amount: float,
                           target_symbol_or_instrument: Optional[Union[str, Instrument]] = "",
                           order_side: Optional[Market] = Market.BUY,
                           buffer_perc: Optional[float] = 0.008
                           ) -> float:
        """
        Only transfer requested amount of target asset to target account. Only trades allowed_spot_transfer_assets

        :param target_account: Target account
        :param target_asset: Target asset
        :param target_amount: Target amount
        :param target_symbol_or_instrument: Symbol or instrument if trading isolated margin
        :param order_side: Order side
        :param buffer_perc: Safety buffer percentage
        :return: The amount transferred in terms of target_asset
        :raises ExecutionException: If any parameter validation failed, if balance insufficient
        """

        instrument, total_free_balance = self.__init_transfer(target_account, order_side, target_asset, target_symbol_or_instrument=target_symbol_or_instrument)
        if total_free_balance <= target_amount:
            raise ExecutionException(
                f"Insufficient balance. Account Free Balance: {target_asset} {total_free_balance}, amount {target_amount}")

        # If SPOT transfer allowed, and balance high enough, then transfer
        if target_asset in self.allowed_spot_assets:
            _, _, already_present_balances = self.__get_balances(target_account, target_asset, target_instrument=instrument, return_already_present_balances=True)
            for balance in already_present_balances:
                if balance[0] == AccountType.SPOT and balance[2] >= target_amount:
                    self.transfer_spot_to_any(target_account, target_asset, target_amount, symbol_or_instrument=target_symbol_or_instrument, websocket_keepalive=True)
                    return target_amount

        apb, _, balances = self.__get_balances(target_account, target_asset, target_instrument=instrument)
        apb2, _, balances_in_terms_of_target_asset = \
            self.__get_balances(target_account, target_asset, target_instrument=instrument, return_converted_balances=True)
        subset_sums = self.__subset_sum(balances_in_terms_of_target_asset, target_amount)
        if not subset_sums:
            raise ExecutionException(f"Insufficient balance to carry out transfer of {target_amount} {target_asset}")

        # Find shorted sum within buffer
        sorted_sums = sorted(subset_sums, key=len)
        shortest_sum = []
        for sorted_sum in sorted_sums:
            total_sum = sum([bal[2] for bal in sorted_sum])
            if total_sum > target_amount * (1 + buffer_perc):
                shortest_sum = sorted_sum
                break
        if not shortest_sum:
            raise ExecutionException(f"Could not find shortest sum for {target_amount} {target_asset}, Best balance: {sorted_sum[0]}")

        # Get amount to debit from each account
        debits = self.__get_debits(shortest_sum, target_amount)
        real_balances = []
        for conversion_balance in shortest_sum:
            for balance in balances:
                if balance[0] == conversion_balance[0] and balance[1] == conversion_balance[1] and balance[3] == conversion_balance[3]:
                    real_balances.append(balance)
        if len(real_balances) != len(shortest_sum):
            raise ExecutionException(f"Could not find real balances for shortest sum: {shortest_sum}")

        # Convert the balances in SPOT
        orders = self.__convert_balances(target_asset, debits, real_balances, order_side=order_side, safety_buffer=buffer_perc)
        final_converted_quantity = orders[-1].quantity if orders[-1].order_side == Market.BUY else orders[-1].transacted_quantity
        # Transfer from SPOT to target
        if target_account != AccountType.SPOT:
            self.transfer_spot_to_any(target_account, target_asset, final_converted_quantity, symbol_or_instrument=instrument, websocket_keepalive=True)
        return final_converted_quantity

    def transfer(self,
                 target_account: AccountType,
                 target_asset: str,
                 target_amount: float,
                 target_symbol_or_instrument: Optional[Union[str, Instrument]] = "",
                 order_side: Optional[Market] = Market.BUY,
                 balance_buffer_perc: Optional[float] = 0.01
                 ) -> float:
        """
        Transfers enough of an asset into an account to make a trade of target_amount. Trades assets as needed.
        Using this function may mean that small balances appear in SPOT as a result of lot rounding
        and required minimums.

        :param target_account: Account to transfer to
        :param target_asset: Asset to transfer
        :param target_amount: Target amount
        :param target_symbol_or_instrument: If isolated margin, the symbol
        :param order_side: BUY or SELL
        :param balance_buffer_perc: Optional safety buffer when checking total free balances. Defaults to 1%
        :return: Total amount transferred into target account
        :raises ExecutionException: If transfer fails or parameters are wrong
        """
        # Validate account type
        if not isinstance(target_account, AccountType):
            raise ExecutionException(f"target_account must be an AccountType. Received {target_account}")

        if order_side not in [Market.BUY, Market.SELL]:
            raise ExecutionException(f"Invalid order side: {order_side}")

        # Validate asset and symbol
        self.__validate_asset_symbol(target_account, target_asset, symbol_or_instrument=target_symbol_or_instrument)

        # Check if already have enough
        # TODO: Balance buffer perc
        if self.__balance_sufficient(target_account, target_asset, target_amount,
                                     symbol_or_instrument=target_symbol_or_instrument):
            logger.info(f"Balance sufficient. No need to transfer")
            return 0.0

        # Get Instrument
        if target_symbol_or_instrument:
            instrument = get_instrument(self.instruments, target_symbol_or_instrument)
            # Cheat and create websocket early
            self.account_manager.create_isolated_margin_socket(instrument)
        else:
            instrument = None

        # Check if we have sufficient balance
        total_free_balance = self.account_manager.total_free_account_value(target_asset, self.quoter,
                                                                           allowed_spot_assets=self.__allowed_spot_transfer_assets)
        if total_free_balance <= target_amount:
            raise ExecutionException(
                f"Insufficient balance. Account Free Balance: {target_asset} {total_free_balance}, amount {target_amount}")

        asset_in_all_accounts, asset_amount_already_present, already_present_balances = \
            self.__get_balances(target_account, target_asset, target_instrument=instrument,
                                return_already_present_balances=True)

        # Transfer already present balances
        amount_needed = target_amount - asset_amount_already_present
        target_symbol = "" if not instrument else instrument.symbol
        self.__transfer_same_denomination_assets(already_present_balances, amount_needed, target_account, target_asset,
                                                 target_symbol=target_symbol)

        amount_present = self.account_manager.get_balance(target_account, BalanceType.FREE, target_asset,
                                                          symbol_or_instrument=instrument)
        if amount_present >= target_amount:
            logger.info(f"Executed transfer of {target_asset} {amount_needed} to {target_account} without trades")
            return amount_needed

        # Trade for the rest of the required amount
        amount_remaining = target_amount - amount_present
        _, _, balances_in_terms_of_target_asset = \
            self.__get_balances(target_account, target_asset, target_instrument=instrument,
                                return_converted_balances=True)
        debits = self.__get_debits(balances_in_terms_of_target_asset, amount_remaining)

        _, _, balances = self.__get_balances(target_account, target_asset, target_instrument=instrument)
        subset_sums = self.__subset_sum(balances, amount_remaining)
        if not subset_sums:
            raise ExecutionException(f"Insufficient balance to carry out transfer of {target_amount} {target_asset}")

        shortest_sum = min(subset_sums, key=len)
        # At the end of this one, required balance should end up in SPOT account
        orders = self.__convert_balances(target_asset, debits, shortest_sum, order_side=order_side, safety_buffer=balance_buffer_perc)
        final_converted_quantity = orders[-1].quantity

        # Transfer from SPOT to target
        if target_account != AccountType.SPOT:
            self.transfer_spot_to_any(target_account, target_asset, final_converted_quantity, symbol_or_instrument=instrument, websocket_keepalive=True)

        # Return final amount transferred to target account
        end_balance = self.account_manager.get_balance(target_account, BalanceType.FREE, target_asset, symbol_or_instrument=instrument)
        logger.info(f"Executed transfer of {target_asset} {end_balance - asset_amount_already_present} to {target_account}")
        return end_balance - asset_amount_already_present

    def transfer_isolated_margin_to_spot(self, asset: str, amount: float, symbol: str) -> int:
        """
        Transfers an isolated margin amount to spot. Handles listening for events to confirm the transaction took place

        :param asset: Asset to transfer
        :param amount: Amount
        :param symbol: Isolated margin symbol
        :return: Transaction id
        :raises ExecutionException: If transaction times out
        """
        transfer_uuid = self.__new_transfer_global()
        trans_handler = self.__balance_update_template(transfer_uuid, AccountType.SPOT, amount, asset,
                                                       symbol_from_iso=symbol)
        self.account_manager.register_balance_update_callback(trans_handler)

        txid = self.binance_client.transfer_isolated_margin_to_spot(asset=asset, amount=str(amount), symbol=symbol)["tranId"]
        self.__wait_for_transfer(transfer_uuid, asset, amount, AccountType.ISOLATED_MARGIN, AccountType.SPOT, txid=txid, instrument_or_symbol=symbol)
        self.account_manager.deregister_balance_update_callback(trans_handler)
        del globals()[transfer_uuid]
        return int(txid)

    def transfer_spot_to_margin(self, asset: str, amount: float) -> int:
        """
        Transfers spot amount to margin. Handles listening for events to confirm the transaction took place.

        :param asset: Asset to transfer
        :param amount: Amount
        :return: Transaction id
        """
        transfer_uuid = self.__new_transfer_global()
        trans_handler = self.__balance_update_template(transfer_uuid, AccountType.MARGIN, amount, asset,
                                                       symbol_from_iso="")
        self.account_manager.register_balance_update_callback(trans_handler)
        txid = self.binance_client.transfer_spot_to_margin(asset=asset, amount=str(amount))["tranId"]
        self.__wait_for_transfer(transfer_uuid, asset, amount, AccountType.SPOT, AccountType.MARGIN, txid=txid)
        self.account_manager.deregister_balance_update_callback(trans_handler)
        del globals()[transfer_uuid]
        return int(txid)

    def transfer_spot_to_isolated_margin(self, asset: str, amount: float, symbol: str,
                                         websocket_keepalive: Optional[bool] = False) -> int:
        """
        Transfers spot amount to isolated margin. Handles listening for events to confirm the transaction took place.

        :param asset: Asset to transfer
        :param amount: Amount
        :param symbol: Isolated margin symbol
        :param websocket_keepalive: By default will tear down websocket. Set True if you are actively trading the symbol as the
                                        websocket takes several milliseconds to spawn
        :return: Transaction id
        """
        transfer_uuid = self.__new_transfer_global()
        self.account_manager.create_isolated_margin_socket(symbol)
        trans_handler = self.__balance_update_template(transfer_uuid, AccountType.ISOLATED_MARGIN, amount, asset,
                                                       symbol_from_iso=symbol)
        self.account_manager.register_balance_update_callback(trans_handler)
        txid = self.binance_client.transfer_spot_to_isolated_margin(asset=asset, amount=str(amount), symbol=symbol)["tranId"]
        self.__wait_for_transfer(transfer_uuid, asset, amount, AccountType.SPOT, AccountType.ISOLATED_MARGIN, txid=txid, instrument_or_symbol=symbol)
        self.account_manager.deregister_balance_update_callback(trans_handler)
        del globals()[transfer_uuid]
        if not websocket_keepalive:
            self.account_manager.stop_isolated_margin_socket(symbol)
        return int(txid)

    def transfer_margin_to_spot(self, asset: str, amount: float) -> int:
        """
        Transfers margin amount to spot. Handles listening for events to confirm the transaction took place.

        :param asset: Asset to transfer
        :param amount: Amount
        :return: Transaction id
        """
        transfer_uuid = self.__new_transfer_global()
        trans_handler = self.__balance_update_template(transfer_uuid, AccountType.SPOT, amount, asset,
                                                       symbol_from_iso="")
        self.account_manager.register_balance_update_callback(trans_handler)
        txid = self.binance_client.transfer_margin_to_spot(asset=asset, amount=str(amount))["tranId"]
        self.__wait_for_transfer(transfer_uuid, asset, amount, AccountType.MARGIN, AccountType.SPOT, txid=txid)
        self.account_manager.deregister_balance_update_callback(trans_handler)
        del globals()[transfer_uuid]
        return int(txid)

    def transfer_isolated_margin_to_margin(self, asset: str, amount: float, symbol: str) -> List[int]:
        """
        Transfers isolated margin balance to margin in 2 steps, ISO -> SPOT -> MARGIN.

        :param asset: Asset to transfer
        :param amount: Amount
        :param symbol: Isolated margin symbol
        :return: List of transaction ids
        """
        txid1 = self.transfer_isolated_margin_to_spot(asset, amount, symbol)
        txid2 = self.transfer_spot_to_margin(asset, amount)
        return [txid1, txid2]

    def transfer_isolated_margin_to_isolated_margin(self, asset: str, amount: float, from_symbol: str,
                                                    to_symbol: str,
                                                    websocket_keepalive: Optional[bool] = True) -> List[int]:
        """
        Transfers isolated margin balance to another isolated margin account in 2 steps, ISO1 -> SPOT -> ISO2.

        :param asset: Asset to transfer
        :param amount: Amount
        :param from_symbol: Isolated margin symbol #1
        :param to_symbol: Isolated margin symbol #2
        :param websocket_keepalive: By default will tear down websocket. Set True if you are actively trading the symbol as the
                                        websocket takes several milliseconds to spawn
        :return: List of transaction ids
        """
        self.account_manager.create_isolated_margin_socket(to_symbol)
        txid1 = self.transfer_isolated_margin_to_spot(asset, amount, from_symbol)
        txid2 = self.transfer_spot_to_isolated_margin(asset, amount, to_symbol, websocket_keepalive=websocket_keepalive)
        if not websocket_keepalive:
            self.account_manager.stop_isolated_margin_socket(to_symbol)
        return [txid1, txid2]

    def transfer_margin_to_isolated_margin(self, asset: str, amount: float, symbol: str,
                                           websocket_keepalive: Optional[bool] = False) -> List[int]:
        """
        Transfers margin balance to isolated margin in 2 steps, MARGIN -> SPOT -> ISO.

        :param asset: Asset to transfer
        :param amount: Amount
        :param symbol: Isolated margin symbol
        :param websocket_keepalive: By default will tear down websocket. Set True if you are actively trading the symbol as the
                                        websocket takes several milliseconds to spawn
        :return: List of transaction ids
        """
        self.account_manager.create_isolated_margin_socket(symbol)
        txid1 = self.transfer_margin_to_spot(asset, amount)
        txid2 = self.transfer_spot_to_isolated_margin(asset, amount, symbol)
        if not websocket_keepalive:
            self.account_manager.stop_isolated_margin_socket(symbol)
        return [txid1, txid2]

    def transfer_any_to_spot(self,
                             account: AccountType,
                             asset: str,
                             amount: float,
                             symbol_or_instrument: Optional[Union[str, Instrument]] = None
                             ) -> int:
        """
        Transfer from any type of account to spot

        :param account: Origin account
        :param asset: Asset to transfer
        :param amount: Amount to transfer
        :param symbol_or_instrument: Symbol or Instrument if transferring from isolated margin
        :return: Transaction id
        :raises ExecutionException: If not symbol specified for isolated margin, if unknown account type
        """

        if account == AccountType.SPOT:
            return -1
        elif account == AccountType.MARGIN:
            txid = self.transfer_margin_to_spot(asset, amount)
        elif account == AccountType.ISOLATED_MARGIN:
            if not symbol_or_instrument:
                raise ExecutionException("Must specify symbol if transferring from isolated margin")
            instrument = get_instrument(self.instruments, symbol_or_instrument)
            txid = self.transfer_isolated_margin_to_spot(asset, amount, instrument.symbol)
        else:
            raise ExecutionException(f"Unknown account type: {account}")
        return txid

    def transfer_spot_to_any(self,
                             account: AccountType,
                             asset: str,
                             amount: float,
                             symbol_or_instrument: Optional[Union[str, Instrument]] = None,
                             websocket_keepalive: Optional[bool] = False
                             ) -> int:
        """
        Transfer from spot to any account.

        :param account: Target account
        :param asset: Target asset
        :param amount: Target amount to transfer
        :param symbol_or_instrument: Symbol or instrument for isolated margin
        :param websocket_keepalive: Whether or not to keep isolated margin websocket alive
        :return: Transaction id
        :raises ExecutionException: If not symbol specified for isolated margin, if unknown account type
        """
        if account == AccountType.SPOT:
            return -1
        elif account == AccountType.MARGIN:
            txid = self.transfer_spot_to_margin(asset, amount)
        elif account == AccountType.ISOLATED_MARGIN:
            if not symbol_or_instrument:
                raise ExecutionException("Must specify symbol if transferring from isolated margin")
            instrument = get_instrument(self.instruments, symbol_or_instrument)
            txid = self.transfer_spot_to_isolated_margin(asset, amount, instrument.symbol,
                                                  websocket_keepalive=websocket_keepalive)
        else:
            raise ExecutionException("Must specify symbol if transferring from isolated margin")
        return txid

    def enter_margin_position(self,
                              account_type: AccountType,
                              instrument_or_symbol: Union[str, Instrument],
                              position_size_base: float,
                              order_side: Market,
                              margin_deposit: float,
                              deposit_denomination: str,
                              stop_loss: float,
                              validate_borrow: Optional[bool] = False,
                              client_order_id: Optional[str] = ""
                              ) -> Position:
        """
        Enters a margin position

        :param account_type: Margin or isolated
        :param instrument_or_symbol: Instrument to trade
        :param position_size_base: The position size in terms of base asset
        :param order_side: BUY or SELL
        :param margin_deposit: Amount of initial margin to fund
        :param deposit_denomination: Denomination of deposit
        :param stop_loss: Stop loss
        :param validate_borrow: Optionally validate if amount needed to borrow to fulfill position size is allowed
        :param client_order_id: Optional client order id
        :return: Position object
        """

        instrument = get_instrument(self.instruments, instrument_or_symbol)
        self.__validate_margin_position(order_side, deposit_denomination, instrument, account_type)

        logger.info(f"Entering Margin position. {account_type} / {order_side} / {instrument.symbol} "
                    f"with position size {position_size_base} {instrument.base_asset}, Deposit {margin_deposit} {deposit_denomination}, "
                    f"Deposit value: {self.conversion_chain.convert(deposit_denomination, self.account_manager.account_denomination, amount=margin_deposit)}"
                    f" {self.account_manager.account_denomination}")

        margin_ratio = self.account_manager.margin_account.margin_ratio if account_type == AccountType.MARGIN else instrument.isolated_margin_ratio
        asset_to_borrow = instrument.base_asset if deposit_denomination == instrument.quote_asset else instrument.quote_asset
        price = self.quoter.get_ask(instrument, fall_back_to_api=True) if order_side == Market.BUY else self.quoter.get_bid(instrument, fall_back_to_api=True)

        # Deposit margin and get max borrow amount
        margin_deposit = self.transfer_from_spot(account_type, deposit_denomination, margin_deposit, target_symbol_or_instrument=instrument, order_side=order_side)
        if validate_borrow:
            if account_type == AccountType.MARGIN:
                max_borrow = float(self.binance_client.get_max_margin_loan(asset=asset_to_borrow)['amount'])
            else:
                max_borrow = float(self.binance_client.get_max_margin_loan(asset=asset_to_borrow, isolatedSymbol=instrument.symbol)['amount'])
        else:
            if order_side == Market.BUY:
                max_borrow = (margin_deposit * margin_ratio) * price
            else:
                max_borrow = (margin_deposit * margin_ratio) / price

        # Enter position, Borrow, buy, set stop loss
        amount_to_borrow, order, pos_size_actual, sl_order, txid = self.__enter_margin_buy_or_sell(
            order_side, instrument, account_type, max_borrow, margin_deposit, position_size_base, asset_to_borrow, deposit_denomination, client_order_id, stop_loss, price
        )

        entry_value = self.conversion_chain.convert(deposit_denomination, self.account_manager.account_denomination, amount=margin_deposit, order_type=order_side)
        position = Position()
        position.instrument = instrument
        position.position_id = client_order_id
        position.account_type = account_type
        position.side = order_side
        position.position_size = pos_size_actual
        position.borrow_txid = txid
        position.borrow_amount = amount_to_borrow
        position.borrow_denomination = asset_to_borrow
        position.margin_deposit = margin_deposit
        position.deposit_denomination = deposit_denomination
        position.margin_buy_amount = order.quantity
        position.stop_order = sl_order
        position.entry_denomination = self.account_manager.account_denomination
        position.entry_value = entry_value
        return position

    def __enter_margin_buy_or_sell(self,
                                   order_side: Market,
                                   instrument: Instrument,
                                   account_type: AccountType,
                                   max_borrow: float,
                                   margin_deposit: float,
                                   position_size_base: float,
                                   asset_to_borrow: str,
                                   deposit_denomination: str,
                                   client_order_id: str,
                                   stop_loss: float,
                                   price: float) -> Tuple[float, Order, float, Order, int]:
        """
        Helper for enter_margin_position.

        :param instrument: Instrument
        :param account_type: Account type
        :param max_borrow: Max borrow amount
        :param margin_deposit: Margin deposit amount
        :param position_size_base: Position size in terms of base asset
        :param asset_to_borrow: Asset to borrow
        :param deposit_denomination: margin_deposit denomination
        :param client_order_id: Optional client order id
        :param stop_loss: Stop loss
        :param price: instrument price
        :return: amount_to_borrow, order, pos_size_actual, sl_order, txid
        """
        if order_side == Market.BUY:
            amount_to_borrow = ((margin_deposit * (position_size_base / margin_deposit)) - margin_deposit) * price
        else:
            position_size = position_size_base * price
            amount_to_borrow = ((margin_deposit * (position_size / margin_deposit)) - margin_deposit) / price

        amount_to_borrow = round(amount_to_borrow, instrument.digits)  # self.round_lot(instrument, amount_to_borrow, strategy=ROUND_UP)

        if amount_to_borrow > max_borrow:
            raise ExecutionException(f"Amount to borrow {amount_to_borrow} {asset_to_borrow} is greater than allowed amount {max_borrow}")

        txid = self.borrow(account_type, asset_to_borrow, amount_to_borrow, instrument_or_symbol=instrument)
        if order_side == Market.BUY:
            quantity = self.round_lot(instrument, amount_to_borrow / price)
        else:
            quantity = self.round_lot(instrument, amount_to_borrow)

        order, pos_size_actual, sl_order = self.__place_margin_entry_order(
            instrument, margin_deposit, deposit_denomination, account_type, order_side, quantity, stop_loss, client_order_id
        )
        return amount_to_borrow, order, pos_size_actual, sl_order, txid

    def __place_margin_entry_order(self,
                                   instrument: Instrument,
                                   margin_deposit: float,
                                   deposit_denomination: str,
                                   account_type: AccountType,
                                   order_side: Market,
                                   quantity: float,
                                   stop_loss: float,
                                   client_order_id: str
                                   ):
        """
        Places the initial margin buy and stop loss orders

        :param instrument: Instrument
        :param margin_deposit: Margin deposit amount
        :param deposit_denomination: margin_deposit denomination
        :param account_type: Account type
        :param order_side: Order side
        :param quantity: Amount to buy
        :param stop_loss: Stop loss
        :param client_order_id: Optional client order id
        :return: order, pos_size_actual, sl_order
        """
        curr_balance_begin = self.account_manager.get_balance(account_type, BalanceType.FREE, deposit_denomination, symbol_or_instrument=instrument)
        try:
            order = self.market_order(instrument, account_type, order_side, quantity, client_order_id=client_order_id, websocket_keepalive=True)
        except ExecutionException as e:
            # Try once with a slightly lower amount
            err = str(e)
            if "insufficient balance" in err:
                quantity *= 0.999
                order = self.market_order(instrument, account_type, order_side, quantity, client_order_id=client_order_id, websocket_keepalive=True)
            else:
                raise e

        while (curr_balance := self.account_manager.get_balance(account_type, BalanceType.FREE, deposit_denomination,
                                                                symbol_or_instrument=instrument)) == curr_balance_begin:
            pass
        pos_size_actual = margin_deposit + order.quantity
        if account_type == AccountType.ISOLATED_MARGIN:
            pos_size_actual = curr_balance
        else:
            if pos_size_actual > curr_balance:
                pos_size_actual = curr_balance

        if order_side == Market.BUY:
            sl_order = self.stop_loss_for_market(instrument, account_type, order_side, pos_size_actual, stop_loss)
        else:
            pos_size_actual_at_stop_loss = pos_size_actual / stop_loss
            pos_size_actual_at_stop_loss *= 0.999 # Small buffer so we dont get 'insufficient funds'
            pos_size_actual_at_stop_loss = self.round_lot(instrument, pos_size_actual_at_stop_loss)
            sl_order = self.stop_loss_for_market(instrument, account_type, order_side, pos_size_actual_at_stop_loss, stop_loss)
        return order, pos_size_actual, sl_order

    def exit_margin_position(self, position: Position, exit_to_spot_currency: Optional[str] = "") -> Position:
        """
        Ugly function that exits a margin position.

        :param position: Position object
        :param exit_to_spot_currency: Optionally exit to a spot currency
        :return:
        """
        logger.info(f"Closing margin position {position}")
        bal = lambda balance_type, asset, use_api: self.account_manager.get_balance(position.account_type, balance_type, asset, symbol_or_instrument=position.instrument,
                                                                                    use_api=use_api)

        def wait_for_repay(asset: str) -> None:
            while bal(BalanceType.BORROWED, asset, False):
                pass
            return

        def wait_for_balance_change(initial_balance: float, asset: str) -> float:
            while (balance := bal(BalanceType.FREE, asset, False)) == initial_balance:
                pass
            return balance

        def try_cancel(stop_order: Order) -> None:
            """
            Tries to cancel an order 3 times before giving up
            :param stop_order: Order to cancel
            :return: None
            """
            cancel_counter = 0
            while cancel_counter < 3:
                try:
                    self.cancel_order(stop_order)
                except ExecutionException:
                    sleep(0.5)
                    cancel_counter += 1
                else:
                    break
            return

        if position.side == Market.BUY:
            start_base_balance = bal(BalanceType.FREE, position.instrument.base_asset, False)
            try_cancel(position.stop_order)
            wait_for_balance_change(start_base_balance, position.instrument.base_asset)
        else:
            start_quote_balance = bal(BalanceType.FREE, position.instrument.quote_asset, False)
            try_cancel(position.stop_order)
            wait_for_balance_change(start_quote_balance, position.instrument.quote_asset)

        start_base_balance = bal(BalanceType.FREE, position.instrument.base_asset, False)
        start_quote_balance = bal(BalanceType.FREE, position.instrument.quote_asset, False)
        if position.side == Market.BUY:
            trade_size = self.round_lot(position.instrument, position.position_size)
            exit_order = self.market_order(position.instrument, position.account_type, Market.SELL, trade_size, auto_repay_margin=True)
        else:
            def try_close(counter: int) -> Order:
                if counter == 0:
                    multiplier = 1.0
                elif counter == 1:
                    multiplier = 0.998
                else:
                    multiplier = 0.995
                ask = self.quoter.get_ask(position.instrument, fall_back_to_api=True)
                trade_size = (position.position_size / ask) * multiplier
                trade_size = self.round_lot(position.instrument, trade_size)
                return self.market_order(position.instrument, position.account_type, Market.BUY, trade_size, auto_repay_margin=True)
            counter = 0
            exit_order = None
            while counter < 3:
                try:
                    exit_order = try_close(counter)
                except ExecutionException as e:
                    counter += 1
                    sleep(0.5)
                except Exception as e:
                    raise ExecutionException(f"Failed to close order: {e}")
                else:
                    break
        if not exit_order:
            raise ExecutionException(f"Failed to close order")

        position.exit_order = exit_order

        if exit_to_spot_currency:
            if position.side == Market.BUY:
                wait_for_repay(position.instrument.quote_asset)
                base_asset_balance = wait_for_balance_change(start_base_balance, position.instrument.base_asset)
                quote_asset_balance = bal(BalanceType.FREE, position.instrument.quote_asset, True)
            else:
                wait_for_repay(position.instrument.base_asset)
                quote_asset_balance = wait_for_balance_change(start_quote_balance, position.instrument.quote_asset)
                base_asset_balance = bal(BalanceType.FREE, position.instrument.base_asset, True)

            def transfer_to_spot(asset: str, balance: float) -> None:
                self.transfer_any_to_spot(position.account_type, asset, balance, symbol_or_instrument=position.instrument)

            counter = 0
            base_transfer_success = False
            quote_transfer_success = False
            while counter < 3 and (not base_transfer_success or not quote_transfer_success):
                if not base_transfer_success:
                    try:
                        transfer_to_spot(position.instrument.base_asset, base_asset_balance)
                        logger.debug(f"Transferred {base_asset_balance} {position.instrument.base_asset} to SPOT")
                        base_transfer_success = True
                    except BinanceAPIException as e:
                        err = str(e)
                        if "-11015" not in err:
                            raise e
                if not quote_transfer_success:
                    try:
                        transfer_to_spot(position.instrument.quote_asset, quote_asset_balance)
                        logger.debug(f"Transferred {quote_asset_balance} {position.instrument.quote_asset} to SPOT")
                        quote_transfer_success = True
                    except BinanceAPIException as e:
                        err = str(e)
                        if "-11015" not in err:
                            raise e
                if not base_transfer_success or not quote_transfer_success:
                    counter += 1
                    if not base_transfer_success:
                        base_asset_balance = bal(BalanceType.FREE, position.instrument.base_asset, True)
                    if not quote_transfer_success:
                        quote_asset_balance = bal(BalanceType.FREE, position.instrument.quote_asset, True)
                    sleep(0.25)

            if position.side == Market.BUY:
                conversion_chain = self.conversion_chain.set_chain(exit_to_spot_currency, position.instrument.quote_asset)
                execution_chain = self.conversion_chain.execution_chain
                chain_asset_start = get_instrument(self.instruments, conversion_chain[0]).base_asset
                balance_in_terms_of_chain_start = self.conversion_chain.convert(
                    position.instrument.quote_asset, chain_asset_start, amount=quote_asset_balance, order_type=execution_chain[0]
                )
                orders = self.execute_conversion_chain(conversion_chain, execution_chain, balance_in_terms_of_chain_start)  # quote_asset_balance)
                profit = self.conversion_chain.convert(position.instrument.quote_asset, position.entry_denomination, amount=quote_asset_balance) - position.entry_value
            else:
                conversion_chain = self.conversion_chain.set_chain(exit_to_spot_currency, position.instrument.base_asset)
                execution_chain = self.conversion_chain.execution_chain
                orders = self.execute_conversion_chain(conversion_chain, execution_chain, base_asset_balance)
                profit = self.conversion_chain.convert(position.instrument.base_asset, position.entry_denomination, amount=base_asset_balance) - position.entry_value
            position.profit = profit

        logger.info(f"Closed margin position {position}")
        return position

    def cancel_order(self, order: Order) -> None:
        """
        Checks to see if order is open, then cancels

        :param order: The order to cancel
        :return: None
        :raises ExecutionException, CCXTError: If fetching or cancelling the order fails
        """
        ccxt_params = self.__ccxt_params(order.account, auto_repay=False)
        ccxt_symbol = order.instrument.base_asset + '/' + order.instrument.quote_asset
        try:
            fetched_order = self.ccxt_client.fetch_order(order.order_id, symbol=ccxt_symbol, params=ccxt_params)
            fetched_order = order_from_cctx(fetched_order, Exchange.BINANCE, order.instrument, order.account)
        except Exception as e:
            err = str(e)
            raise ExecutionException(f"Fetching order failed with {err}")

        if fetched_order.status == OrderStatus.OPEN:
            self.ccxt_client.cancel_order(order.order_id, symbol=ccxt_symbol, params=ccxt_params)

        return

    def borrow(self, account: AccountType, asset: str, amount: float, instrument_or_symbol: Optional[Union[str, Instrument]] = None) -> int:
        """
        Executes a margin borrow

        :param account: Which margin account
        :param asset: Asset to borrow
        :param amount: Amount to borrow
        :param instrument_or_symbol: Provide symbol is Isolated margin
        :return: The txid
        :raises ExecutionException: If non margin account
        """
        if account not in [AccountType.MARGIN, AccountType.ISOLATED_MARGIN]:
            raise ExecutionException(f"Account {account} is not a margin account")
        if account == AccountType.MARGIN:
            txid = int(self.binance_client.create_margin_loan(asset=asset, amount=str(amount))["tranId"])
            logger.info(f"Created MARGIN loan for {amount} {asset} with TXID{{{txid}}}")
            return txid
        else:
            instrument = get_instrument(self.instruments, instrument_or_symbol)
            txid = int(self.binance_client.create_margin_loan(asset=asset, isIsolated='TRUE', symbol=instrument.symbol, amount=str(amount))["tranId"])
            logger.info(f"Created ISOLATED_MARGIN loan for {amount} [{asset}|{instrument.symbol}] with TXID{{{txid}}}")
            return txid

    def execute_conversion_chain(self,
                                 conversion_chain: ConversionChainType,
                                 execution_chain: List[Market],
                                 initial_quantity: float,
                                 fall_back_to_api: Optional[bool] = True
                                 ) -> List[Order]:
        """
        Executes a conversion chain. Will always use spot account.

        :param conversion_chain: The conversion chain to execute
        :param execution_chain: The market order types
        :param initial_quantity: The starting quantity
        :param fall_back_to_api: For quoter
        :return: List of ordered orders executed
        """

        curr_order = None
        orders: List[Order] = []
        logger.info(f"Executing conversion chain: {conversion_chain}")
        for i, (symbol, exec_type) in enumerate(zip(conversion_chain, execution_chain)):
            instrument = get_instrument(self.instruments, symbol)
            if not curr_order:
                initial_quantity = self.round_lot(instrument, initial_quantity)
                curr_order = self.market_order(symbol, AccountType.SPOT, exec_type, initial_quantity, websocket_keepalive=True)
            else:
                if exec_type == Market.BUY:
                    prev_exec_type = execution_chain[i - 1]
                    ask = self.quoter.get_ask(symbol, fall_back_to_api=fall_back_to_api)
                    if prev_exec_type == Market.SELL:
                        quantity = curr_order.transacted_quantity / ask
                        logger.debug(f"Executing {exec_type} for {symbol}, Prev quantity {curr_order.transacted_quantity}, Quantity: {quantity}, ask {ask}")
                    else:
                        quantity = curr_order.quantity / ask
                elif exec_type == Market.SELL:
                    prev_exec_type = execution_chain[i - 1]
                    bid = self.quoter.get_bid(symbol, fall_back_to_api=fall_back_to_api)
                    if prev_exec_type == Market.BUY:
                        quantity = curr_order.quantity * bid
                        logger.debug(f"Executing {exec_type} for {symbol}, Prev quantity {curr_order.transacted_quantity}, Quantity: {quantity}, bid {bid}")
                    else:
                        quantity = curr_order.transacted_quantity * bid
                        logger.debug(f"Executing {exec_type} for {symbol}, Quantity: {quantity}")
                else:
                    raise ExecutionException(f"Unknown order type: {exec_type}")
                quantity = self.round_lot(instrument, quantity, strategy=ROUND_DOWN)
                curr_order = self.market_order(symbol, AccountType.SPOT, exec_type, quantity, websocket_keepalive=True)
            orders.append(curr_order)

        return orders

    def __subset_sum(self,
                     balances: List[List[Union[AccountType, str, float]]],
                     target: float,
                     partial: Optional[List[List[Union[AccountType, str, float]]]] = [],
                     sums=[]
                     ) -> List[List[List[Union[AccountType, str, float]]]]:
        """
        Calculates all combinations that will get us over required amount

        :param balances: Account balances in terms of desired asset
        :param target: Target amount
        :param partial: Partial sums for recursion
        :return: List of required transfers
        """
        s = sum([p[2] for p in partial])

        if s >= target:
            # print(f"{partial, target}")
            sums.append(partial)

        for i, balance in enumerate(balances):
            n = balances[i]
            remaining = balances[i + 1:]
            self.__subset_sum(remaining, target, partial=partial + [n], sums=sums)

        return sums

    def __get_balances(self,
                       target_account: AccountType,
                       target_asset: str,
                       target_instrument: Optional[Instrument] = None,
                       return_already_present_balances: Optional[bool] = False,
                       return_converted_balances: Optional[bool] = False,
                       spot_only: Optional[bool] = False
                       ) -> Tuple[float, float, List[List[Union[AccountType, str, float]]]]:
        """
        Gets a list of balances in format [AccountType, asset, value_in_target_asset, optional_iso_symbol]

        :param target_account: The account we are transferring to
        :param target_asset: The asset we are transferring
        :param target_instrument: If isolated margin, then the target instrument
        :param return_already_present_balances: Whether or not to only return the balances of the target_asset already available
        :param return_converted_balances: Optionally convert balances into terms of target_asset
        :param spot_only: Only get SPOT balances
        :return: (asset_in_all_accounts, asset_amount_already_present, balances) or
                    (asset_in_all_accounts, asset_amount_already_present, already_present_balances)
        :raises ExecutionException: If could not get value
        """
        instrument = target_instrument
        balances: List[List[Union[AccountType, str, float]]] = []
        asset_in_all_accounts: float = 0.0
        asset_amount_already_present: float = 0.0
        already_present_balances: List[List[Union[AccountType, str, float]]] = []

        def get_value(val_asset, val_target_asset, val_asset_balance) -> float:
            if not return_already_present_balances and return_converted_balances:
                return self.conversion_chain.convert(val_asset, val_target_asset, amount=val_asset_balance)
            if not return_converted_balances:
                return val_asset_balance
            if val_asset == val_target_asset:
                return val_asset_balance
            return 0.0

        for asset in self.account_manager.assets_with_free_balance(AccountType.SPOT):
            asset_balance: float = self.account_manager.get_balance(AccountType.SPOT, BalanceType.FREE, asset)

            if asset not in self.__allowed_spot_transfer_assets:
                # Can't transfer, but can receive. Should track current balance.
                if target_account == AccountType.SPOT and asset == target_asset:
                    asset_amount_already_present += asset_balance
                continue

            value = get_value(asset, target_asset, asset_balance)
            balance = [AccountType.SPOT, asset, value, ""]

            if target_account == AccountType.SPOT and asset == target_asset:
                asset_in_all_accounts += asset_balance
                asset_amount_already_present += asset_balance
                continue
            elif target_account != AccountType.SPOT and asset == target_asset:
                asset_in_all_accounts += asset_balance
                already_present_balances.append(balance)
            else:
                if value:
                    balances.append(balance)

        if not spot_only:
            for asset in self.account_manager.assets_with_free_balance(AccountType.MARGIN):
                asset_balance: float = self.account_manager.get_balance(AccountType.MARGIN, BalanceType.FREE, asset)
                value = get_value(asset, target_asset, asset_balance)
                balance = [AccountType.MARGIN, asset, value, ""]

                if target_account == AccountType.MARGIN and asset == target_asset:
                    asset_in_all_accounts += asset_balance
                    asset_amount_already_present += asset_balance
                    continue
                elif target_account != AccountType.MARGIN and asset == target_asset:
                    asset_in_all_accounts += asset_balance
                    already_present_balances.append(balance)
                else:
                    if value:
                        balances.append(balance)

            for symbol in self.account_manager.assets_with_free_balance(AccountType.ISOLATED_MARGIN):
                for asset in self.account_manager.isolated_margin_balances[symbol].keys():

                    asset_balance: float = \
                        self.account_manager.get_balance(AccountType.ISOLATED_MARGIN, BalanceType.FREE, asset,
                                                         symbol_or_instrument=symbol)
                    value = get_value(asset, target_asset, asset_balance)
                    balance = [AccountType.ISOLATED_MARGIN, asset, value, symbol]

                    if target_account == AccountType.ISOLATED_MARGIN and instrument.symbol == symbol and target_asset == asset:
                        asset_in_all_accounts += asset_balance
                        asset_amount_already_present += asset_balance
                        continue
                    elif (
                            target_account == AccountType.ISOLATED_MARGIN and instrument.symbol != symbol and target_asset == asset) \
                            or (target_account != AccountType.ISOLATED_MARGIN and target_asset == asset):
                        asset_in_all_accounts += asset_balance
                        already_present_balances.append(balance)
                    else:
                        if value:
                            balances.append(balance)

        if return_already_present_balances:
            return asset_in_all_accounts, asset_amount_already_present, already_present_balances
        return asset_in_all_accounts, asset_amount_already_present, balances

    def __transfer_same_denomination_assets(self,
                                            balances: List[List[Union[AccountType, str, float]]],
                                            amount_needed: float,
                                            target_account: AccountType,
                                            target_asset: str,
                                            target_symbol: Optional[str] = ""
                                            ) -> None:
        """
        Shuffles around assets. Threaded.

        :param balances: List of balances. [AccountType, asset, amount, symbol_opt]
        :param amount_needed: Total amount needed
        :return: None when done
        :raises ExecutionException: If account type incorrect
        """
        debit_per_balance: List[float] = []
        cumulative_sum = 0
        asset_digits = self.__get_asset_digits(target_asset)
        for balance in balances:
            if cumulative_sum < amount_needed:
                curr_balance = balance[2]
                remaining_needed = round(amount_needed - cumulative_sum, asset_digits)
                if curr_balance > remaining_needed:
                    debit_per_balance.append(remaining_needed)
                else:
                    debit_per_balance.append(curr_balance)
                cumulative_sum += balance[2]
            else:
                debit_per_balance.append(0)

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(self.__transfer_asset, target_account, target_asset, debit, balance,
                                target_symbol=target_symbol)
                for balance, debit in zip(balances, debit_per_balance)
            ]
            concurrent.futures.wait(futures, timeout=None, return_when=ALL_COMPLETED)

        return

    def __transfer_asset(self,
                         target_account: AccountType,
                         target_asset: str,
                         debit: float,
                         balance: List[Union[AccountType, str, float]],
                         target_symbol: Optional[str] = ""
                         ) -> None:
        """
        Transfers asset of denomination to another account

        :param target_account: Target account
        :param target_asset: Target asset
        :param debit: Amount to debit account
        :param balance: The Balance array
        :param target_symbol: If transferring to isolated margin
        :return: None
        """
        if debit <= 0:
            return

        from_account = balance[0]
        symbol = balance[3]

        if target_account == AccountType.SPOT:
            if from_account == AccountType.MARGIN:
                self.transfer_margin_to_spot(target_asset, debit)
            elif from_account == AccountType.ISOLATED_MARGIN:
                self.transfer_isolated_margin_to_spot(target_asset, debit, symbol)
            else:
                raise ExecutionException(f"Error transferring! Source and destination accounts the same")
        elif target_account == AccountType.MARGIN:
            if from_account == AccountType.ISOLATED_MARGIN:
                self.transfer_isolated_margin_to_margin(target_asset, debit, symbol)
            elif from_account == AccountType.SPOT:
                self.transfer_spot_to_margin(target_asset, debit)
            else:
                raise ExecutionException(f"Error transferring! Source and destination accounts the same")

        elif target_account == AccountType.ISOLATED_MARGIN:
            if from_account == AccountType.SPOT:

                self.transfer_spot_to_isolated_margin(target_asset, debit, target_symbol, websocket_keepalive=True)
            elif from_account == AccountType.MARGIN:
                self.transfer_margin_to_isolated_margin(target_asset, debit, target_symbol, websocket_keepalive=True)
            elif from_account == AccountType.ISOLATED_MARGIN:
                if symbol == target_symbol:
                    raise ExecutionException(f"Error transferring! Source and destination symbols "
                                             f"for isolated margin transfers cannot be the same")
                self.transfer_isolated_margin_to_isolated_margin(target_asset, debit, symbol, target_symbol,
                                                                 websocket_keepalive=True)
        else:
            raise ExecutionException(f"Error transferring! Unknown account type: {target_account}")
        logger.debug(f"Transferring {target_asset} {debit} from {from_account} to {target_account}")

        return

    def __get_debits(self, balances_in_terms_of_target: List[List[Union[AccountType, str, float]]], target_amount
                     ) -> List[float]:
        """
        Get expected debit amounts

        :param balances_in_terms_of_target: Balances list, but in terms of the target asset (return_converted_balances=True in __get_balances)
        :param target_amount: Desired end amount of currency
        :return:
        """
        debits: List[float] = []
        sum: float = 0
        for balance in balances_in_terms_of_target:
            if sum >= target_amount:
                debits.append(0)
            amount = balance[2]
            if sum < target_amount:
                if amount >= target_amount - sum:
                    debits.append(target_amount - sum)
                    sum += target_amount - sum
                else:
                    debits.append(amount)
                    sum += amount
            if debits[-1]:
                debits[-1] = round(debits[-1], self.__get_asset_digits(balance[1]))
        return debits

    def __convert_balances(self,
                           target_asset: str,
                           debits: List[float],
                           balances: List[List[Union[AccountType, str, float]]],
                           order_side: Optional[Market] = Market.BUY,
                           safety_buffer: Optional[float] = 0.005
                           ) -> List[Order]:
        conversion_chains = []
        execution_chains = []
        start_quantities = []
        start_assets = []
        for balance, debit in zip(balances, debits):
            if not debit:
                continue
            self.conversion_chain.set_chain(target_asset, balance[1], order_type=order_side,
                                            highest_liquidity_chain=True)

            conversion_chain = self.conversion_chain.conversion_chain
            execution_chain = self.conversion_chain.execution_chain
            amount_real_to_convert = self.conversion_chain.convert(target_asset, balance[1], amount=debit,
                                                                   order_type=order_side, fall_back_to_api=True)
            min_required = self.conversion_chain.min_quantity_for_chain(balance[1], conversion_chain)
            if min_required > amount_real_to_convert:
                amount_real_to_convert = min_required

            instrument = get_instrument(self.instruments, conversion_chain[0])
            start_asset = balance[1]
            if instrument.quote_asset == start_asset:
                conv_asset = instrument.base_asset
                amount_real_to_convert = \
                    self.conversion_chain.convert(start_asset, conv_asset, amount=amount_real_to_convert, order_type=execution_chain[0], fall_back_to_api=True)
            elif instrument.base_asset != start_asset:
                raise ExecutionException(f"Unknown start asset {start_asset}")
            else:
                conv_asset = instrument.quote_asset

            amount_real_to_convert *= 1 + safety_buffer
            amount_real_to_convert = self.round_lot(instrument, amount_real_to_convert, strategy=ROUND_UP)

            curr_balance = self.account_manager.get_balance(balance[0], BalanceType.FREE, balance[1], symbol_or_instrument=balance[3])

            amount_to_convert_val_in_start_asset = \
                self.conversion_chain.convert(conv_asset, start_asset, amount=amount_real_to_convert, order_type=execution_chain[0], fall_back_to_api=True)
            if curr_balance < amount_to_convert_val_in_start_asset:
                raise ExecutionException(f"{balance[0]} {balance[1]} does not have balance enough to make the required minimum transfer. "
                                         f"Current Balance: {curr_balance} {balance[1]}, Amount Real to Convert: {amount_real_to_convert} {conv_asset}")
            self.transfer_any_to_spot(balance[0], balance[1], amount_real_to_convert, symbol_or_instrument=balance[3])

            start_quantities.append(amount_real_to_convert)
            start_assets.append(balance[1])
            conversion_chains.append(conversion_chain)
            execution_chains.append(execution_chain)

        final_orders: List[Order] = []
        for conversion_chain, exec_chain, start_quantity, start_asset in zip(conversion_chains, execution_chains, start_quantities, start_assets):
            orders = self.execute_conversion_chain(conversion_chain, exec_chain, start_quantity)
            final_orders.append(orders[-1])
        return final_orders

    def __wait_for_transfer(self,
                            transfer_uuid: str,
                            asset: str,
                            amount: float,
                            from_account: AccountType,
                            to_account: AccountType,
                            txid: Optional[float] = -1,
                            instrument_or_symbol: Optional[Union[str, Instrument]] = "") -> None:
        """
        Waits for transaction to complete

        :param transfer_uuid: Unique transaction uuid
        :param asset: Asset to transfer
        :param amount: Amount to transfer
        :param txid: Transaction id
        :param instrument_or_symbol: Optional instrument or symbol
        :return: None
        :raises ExecutionException: If transaction times out
        """

        start = time.time()
        while not globals()[transfer_uuid]:
            end = time.time()
            if end - start > 10:
                if txid > 0:
                    if from_account != AccountType.ISOLATED_MARGIN and to_account != AccountType.ISOLATED_MARGIN:
                        ts = self.account_manager.get_transfer_time(from_account, to_account, asset, amount, txid)
                        return
                    elif to_account == AccountType.ISOLATED_MARGIN and instrument_or_symbol:
                        balance = self.account_manager.get_balance(to_account, BalanceType.NET, asset, symbol_or_instrument=instrument_or_symbol, use_api=True)
                        if balance >= amount:
                            return
                raise ExecutionException(f"Transfer of {asset} {amount} from {from_account} to {to_account} timed out")
        return

    def __wait_for_order(self,
                         transfer_uuid: str,
                         order_type: Market,
                         order_side: Market,
                         symbol: str,
                         quantity: float,
                         account_type: AccountType
                         ) -> None:
        """
        Waits for order to complete

        :param transfer_uuid: Unique transaction uuid
        :param order_type: Order type
        :param order_side: BUY or SELL
        :param symbol: Symbol being traded
        :param quantity: Quantity traded
        :return: None
        :raises ExecutionException: If order times out
        """

        start = time.time()
        while not globals()[transfer_uuid]:
            end = time.time()
            if end - start > 15:
                raise ExecutionException(f"Order / {order_type} / {order_side} / {symbol} / {quantity} / {account_type}"
                                         f" timed out")
        return

    def __balance_update_template(self,
                                  transfer_uuid: str,
                                  account: AccountType,
                                  amount: float,
                                  asset: str,
                                  symbol_from_iso: Optional[str] = ""
                                  ) -> Callable:
        """
        Returns a balance update template

        :param transfer_uuid: Unique ID for the transaction
        :param account: target account
        :param amount: target amount
        :param asset: target asset
        :param symbol_from_iso: Optional symbol for ISO
        :return: Callable
        """

        def balance_update_callback(msg, account_type: AccountType, symbol=""):
            if account_type == account:
                if float(msg["d"]) == amount and msg["a"] == asset:
                    if account_type == AccountType.ISOLATED_MARGIN:
                        if symbol == symbol_from_iso:
                            globals()[transfer_uuid] = True
                    else:
                        globals()[transfer_uuid] = True
            return

        balance_update_callback.__name__ = f"__balance_update_callback_{amount}_{asset}_{transfer_uuid}"
        return balance_update_callback

    def __validate_asset_symbol(self,
                                account_type: AccountType,
                                asset: str,
                                symbol_or_instrument: Optional[Union[str, Instrument]] = ""
                                ) -> None:
        """
        Validates asset and symbol, raises exception if validation fails

        :param account_type: Account type to check
        :param asset: Asset to check
        :param symbol_or_instrument: Optional symbol for isolated margin
        :return: None
        :raises ExecutionException: If validation fails
        """
        if account_type == AccountType.SPOT:
            if asset not in self.client.get_all_assets():
                raise ExecutionException(f"Asset {asset} is not a valid spot asset")
        elif account_type == AccountType.MARGIN:
            if asset not in self.client.get_all_margin_assets():
                raise ExecutionException(f"Asset {asset} is not a valid spot asset")
        elif account_type == AccountType.ISOLATED_MARGIN:
            if not symbol_or_instrument:
                raise ExecutionException("Symbol must be provided for isolated margin")
            symbol = get_instrument(self.instruments, symbol_or_instrument).symbol
            if symbol not in self.__isolated_margin_pairs:
                raise ExecutionException(f"{symbol} is not an isolated margin pair")
        else:
            raise ExecutionException(f"Unknown account type: {account_type}")
        return

    def __validate_margin_position(self, order_side: Market, deposit_denomination: str, instrument: Instrument, account_type: AccountType) -> None:
        """
        Validates an entry into margin position by performing various parameter checks

        :param order_side: BUY or SELL
        :param deposit_denomination: Margin deposit denomination
        :param instrument: Target instrument
        :param account_type: Margin or isolated
        :return: None
        :raises ExecutionException: If order side invalid, if depositing wrong type of asset for order side, if denomination not in pair,
                                        if account is not margin, if margin not allowed for instrument
        """

        if order_side not in [Market.BUY, Market.SELL]:
            raise ExecutionException(f"Invalid order side: {order_side}")
        if order_side == Market.BUY and deposit_denomination == instrument.quote_asset:
            raise ExecutionException(f"Should not be depositing quote asset for a long")
        if order_side == Market.SELL and deposit_denomination == instrument.base_asset:
            raise ExecutionException(f"Should not be depositing base asset for a short")
        if deposit_denomination != instrument.base_asset and deposit_denomination != instrument.quote_asset:
            raise ExecutionException(f"Deposit asset neither base nor quote")
        if account_type not in [AccountType.MARGIN, AccountType.ISOLATED_MARGIN]:
            raise ExecutionException(f"Account {account_type} is not a margin account")
        if account_type == AccountType.ISOLATED_MARGIN and not instrument.isolated_margin_allowed:
            raise ExecutionException(f"Isolated margin not allowed for {instrument.symbol}")
        return

    def __balance_sufficient(self,
                             account_type: AccountType,
                             asset: str,
                             quantity: float,
                             symbol_or_instrument: Optional[Union[str, Instrument]] = ""
                             ) -> bool:
        """
        Returns true if balance is sufficient to make a trade, false otherwise

        :param account_type: type of account
        :param asset: asset to check
        :param quantity: Desired quantity
        :param symbol_or_instrument: Symbol or instrument for isolated margin
        :return: True if balance sufficient, false otherwise
        """
        if account_type == AccountType.SPOT:
            free_spot_assets = self.account_manager.assets_with_free_balance(AccountType.SPOT)
            # Nothing to do
            if asset in free_spot_assets and self.account_manager.get_balance(AccountType.SPOT, BalanceType.FREE,
                                                                              asset) >= quantity:
                return True
        elif account_type == AccountType.MARGIN:
            free_margin_assets = self.account_manager.assets_with_free_balance(AccountType.MARGIN)
            # Nothing to do
            if asset in free_margin_assets and self.account_manager.get_balance(AccountType.MARGIN, BalanceType.FREE,
                                                                                asset) >= quantity:
                return True
        elif account_type == AccountType.ISOLATED_MARGIN:
            symbol = get_instrument(self.instruments, symbol_or_instrument).symbol
            # Nothing to do

            free_iso_margin_symbols = self.account_manager.assets_with_free_balance(AccountType.ISOLATED_MARGIN)
            if symbol in free_iso_margin_symbols:
                if asset in self.account_manager.isolated_margin_balances[symbol].keys() and \
                        self.account_manager.isolated_margin_balances[symbol][asset][BalanceType.FREE] >= quantity:
                    return True
        return False

    def __init_transfer(self,
                        target_account: AccountType,
                        order_side: Market,
                        target_asset: str,
                        target_symbol_or_instrument: Optional[Union[Instrument, str]] = None) -> Tuple[Union[Instrument, None], float]:
        """
        Initializes transfer functions

        :param target_account: Target account
        :param order_side: Order side
        :param target_asset: Target asset
        :param target_symbol_or_instrument: Target instrument if isolated margin
        :return: Instrument, Total free account balance
        """
        # Validate account type
        if not isinstance(target_account, AccountType):
            raise ExecutionException(f"target_account must be an AccountType. Received {target_account}")

        if order_side not in [Market.BUY, Market.SELL]:
            raise ExecutionException(f"Invalid order side: {order_side}")

        if target_account == AccountType.ISOLATED_MARGIN and not target_symbol_or_instrument:
            raise ExecutionException("Target symbol must be provided with isolated margin")

        # Validate asset and symbol
        self.__validate_asset_symbol(target_account, target_asset, symbol_or_instrument=target_symbol_or_instrument)

        # Get Instrument
        if target_symbol_or_instrument:
            instrument = get_instrument(self.instruments, target_symbol_or_instrument)
            # Cheat and create websocket early
            self.account_manager.create_isolated_margin_socket(instrument)
        else:
            instrument = None

        # Check if we have sufficient balance
        total_free_balance = self.account_manager.total_free_account_value(target_asset, self.quoter,
                                                                           allowed_spot_assets=self.__allowed_spot_transfer_assets)
        return instrument, total_free_balance

    def __get_asset_digits(self, asset: str) -> int:
        """
        Gets the digits we should round asset calculations to.

        :param asset: Asset
        :return: Digits
        :raises ExecutionException: If asset not found
        """
        for instrument in self.instruments:
            if instrument.base_asset == asset:
                return instrument.digits
        raise ExecutionException(f"Invalid asset! {asset}")

    def __new_transfer_global(self) -> str:
        """
        Creates a global variable with shortuuid as name.

        :return: Variable name
        """
        transfer_uuid = shortuuid.uuid()
        globals()[transfer_uuid] = False
        return transfer_uuid

    def __order_listener_template(self,
                                  transfer_uuid: str,
                                  account: AccountType,
                                  amount: float,
                                  symbol: str
                                  ) -> Callable:
        """
        Returns a function used as a callback for order events. Passed to AccountManager

        :param transfer_uuid: Unique transfer identifier
        :param account: Event from account
        :param amount: Quantity to be filled
        :param symbol: The traded symbol
        :return: Callable
        """

        def order_callback(order: Order):
            if order.account == account:
                if order.status == OrderStatus.FILLED:
                    if order.instrument.symbol == symbol:
                        if order.quantity == amount:
                            globals()[transfer_uuid] = True
                            globals()[transfer_uuid + "_order"] = order
                            logger.debug(f"Caught order: {order}")
            return

        order_callback.__name__ = f"__order_callback_{amount}_{symbol}_{transfer_uuid}"
        return order_callback

    def __ccxt_params(self, account_type: AccountType, client_order_id: Optional[str] = "", auto_repay: Optional[bool] = True) -> Dict[str, str]:
        """
        Builds param object for CCXT

        :param account_type: Account type
        :param client_order_id: Optional client order id
        :param auto_repay: Whether or not to set auto_repay
        :return: params dict
        """
        params = {}
        if client_order_id:
            params['newClientOrderId'] = str(client_order_id)
        if account_type == AccountType.MARGIN:
            params['type'] = 'margin'
            if auto_repay:
                params['sideEffectType'] = 'AUTO_REPAY'
        if account_type == AccountType.ISOLATED_MARGIN:
            params['type'] = 'margin'
            params['isIsolated'] = 'TRUE'
            if auto_repay:
                params['sideEffectType'] = 'AUTO_REPAY'
        return params
