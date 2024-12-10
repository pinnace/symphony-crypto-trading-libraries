from symphony.enum import Exchange, OrderStatus, Market, AccountType, BalanceType
from symphony.data_classes import Order, MarginAccount, Instrument
from symphony.exceptions import AccountException
from symphony.client import BinanceClient
from symphony.quoter import BinanceRealTimeQuoter
from symphony.borg import Borg
from symphony.utils import order_from_binance_websocket, order_from_binance_api, get_instrument, \
    order_from_cctx, order_model_from_order, insert_or_update_order, get_symbol
from binance.client import Client
from symphony.data_classes import ConversionChain
from binance.streams import ThreadedWebsocketManager
from binance.exceptions import BinanceAPIException
from typing import Dict, Optional, Union, List, Generator, Callable
from symphony.config import USE_MODIN, LOG_LEVEL
from symphony.utils.misc import chunker
import concurrent.futures
from time import sleep
from concurrent.futures._base import ALL_COMPLETED
from twisted.internet import reactor
from ccxt.base.errors import BadSymbol, ExchangeError
import itertools
import logging
import copy

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd
logger = logging.getLogger(__name__)


class BinanceAccountManager(Borg):

    def __init__(self,
                 client: BinanceClient,
                 write_orders: Optional[bool] = False,
                 historical_orders_mode: Optional[bool] = False,
                 isolated_margin_pairs: Optional[Union[str, Instrument, List[str], List[Instrument]]] = [],
                 create_isolated_margin_accounts: Optional[bool] = False,
                 account_denomination: Optional[str] = "EUR",
                 log_level: int = LOG_LEVEL
                 ):
        """
        Binance Account Manager. Populates balances and provides utility functions therein.
        Manages websocket connections for live updates.

        Note that isolated margin sockets must be explicitly specified either on initialization or through exposed
        functions. Client cannot handle listening on all pairs.

        :param client: Binance Client class
        :param write_orders: Optionally writes orders to configured database
        :param historical_orders_mode: If set, this instantiation will only fetch historical orders and send to the configured
                                        database. This will take some time (30m - 1hr). Exits after.
        :param isolated_margin_pairs: Listens by default on no pairs, specify pairs to listen on
        :param create_isolated_margin_accounts: Optionally initialize isolated margin accounts for all assets
        :param account_denomination: Account denomination
        :param log_level: Optional log level
        """
        Borg.__init__(self)

        self.symphony_client: BinanceClient = client
        self.__sub_client: Client = self.symphony_client.binance_client
        self.ss_client = self.__sub_client

        self.exchange: Exchange = self.symphony_client.exchange
        self.maker_commission: float = 0.0
        self.taker_commission: float = 0.0
        self.account_denomination: str = account_denomination
        self.__historical_orders_mode: bool = historical_orders_mode
        self.__write_orders: bool = write_orders
        self.__proxy_instances: List = []

        # Spot
        self.spot_balances: Dict[str, Dict[BalanceType, float]] = {}
        self.open_spot_orders: List[Order] = []
        self.filled_spot_orders: List[Order] = []

        # Margin
        self.margin_account: MarginAccount = None
        self.margin_balances: Dict[str, Dict[BalanceType, float]] = {}
        self.open_margin_orders: List[Order] = []
        self.filled_margin_orders: List[Order] = []

        # Isolated Margin
        self.isolated_margin_balances: Dict[str, Dict[str, Dict[BalanceType, float]]] = {}
        self.isolated_margin_levels: Dict[str, float] = {}
        self.open_isolated_margin_orders: List[Order] = []
        self.filled_isolated_margin_orders: List[Order] = []
        self.__isolated_margin_pairs: List[str] = []  # all pairs
        self.__user_isolated_margin_pairs: List[str] = []  # Pairs we are listening on
        self.__isolated_margin_conn_keys: Dict[str, str] = {}

        # Callbacks
        self.__order_callbacks: List[Callable] = []
        self.__balance_update_callbacks: List[Callable] = []

        # Seed balances with API
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # if self.__fetch_historical_orders:
            #    futures.append(executor.submit(start_proxies))
            futures.append(executor.submit(self.__set_spot_and_initial_account_info))
            futures.append(executor.submit(self.__set_open_spot_orders))
            futures.append(executor.submit(self.__set_margin_balances_and_account))
            futures.append(executor.submit(self.__set_isolated_margin_balances,
                                           create_isolated_margin_account=create_isolated_margin_accounts))

            concurrent.futures.wait(futures, timeout=None, return_when=ALL_COMPLETED)

        if self.__historical_orders_mode:
            self.__set_historical_orders(write_orders=self.__write_orders)

        # Create websockets
        self.__socket_manager = ThreadedWebsocketManager(api_key=self.symphony_client.api_key, api_secret=self.symphony_client.secret_key)
        self.__socket_manager.start()
        self.__spot_trading_conn_key = self.__socket_manager.start_user_socket(self.__handle_spot_event)
        self.__cross_margin_conn_key = self.__socket_manager.start_margin_socket(self.__handle_cross_margin_event)

        # SETUP INITIAL ISOLATED MARGIN SOCKETS
        # Standardize user provided isolated margin pairs
        if isolated_margin_pairs:
            self.__user_isolated_margin_pairs += self.__extract_pairs(isolated_margin_pairs)

        # Quick sanity check
        for user_pair in self.__user_isolated_margin_pairs:
            if user_pair not in self.__isolated_margin_pairs:
                raise AccountException(f"{user_pair} is not a valid isolated margin pair!")

        if self.__user_isolated_margin_pairs:
            for user_pair in self.__user_isolated_margin_pairs:
                self.create_isolated_margin_socket(user_pair)

        self.__isolated_margin_pairs = [instrument.symbol for instrument in self.symphony_client.instruments if instrument.isolated_margin_allowed]

        logger.setLevel(log_level)
        logger.info(f"Account manager initialized. Exchange: {self.exchange}")

        return

    @property
    def isolated_margin_pairs(self) -> List[str]:
        return self.__isolated_margin_pairs

    def get_order(self,
                  symbol_or_instrument: Optional[Union[Instrument, str]] = None,
                  order_id: Optional[int] = 0
                  ) -> Union[List[Order], Order, None]:
        """
        Get an order from the cache from either a symbol or an id

        :param symbol_or_instrument: Optional symbol or instrument
        :param order_id: Optional order id
        :return: The order if one is found, else None
        :raises
        """
        if symbol_or_instrument and order_id:
            raise AccountException(f"Cannot specify both symbol and order id")
        self.__set_open_spot_orders()
        if isinstance(symbol_or_instrument, Instrument):
            symbol = symbol_or_instrument.symbol
        else:
            symbol = symbol_or_instrument

        if not order_id:
            order_or_orders = [order for order in self.orders if order.instrument.symbol == symbol]
            if not len(order_or_orders):
                return None
            if len(order_or_orders) == 1:
                return order_or_orders[0]
            return order_or_orders
        else:
            return [order for order in self.orders if order.order_id == order_id][0]

    def get_order_by_quantity(self, account_type: AccountType, instrument_or_symbol: Union[str, Instrument], order_side: Market, amount: float) -> Union[Order, None]:
        """
        Query a closed order by the quantity

        :param account_type: Account to query
        :param instrument_or_symbol: Symbol traded
        :param order_side: BUY or SELL
        :param amount: Amount transacted
        :return: The order if found, else None
        :raises AccountException: If parameter types unknown
        """
        if not isinstance(account_type, AccountType):
            raise AccountException(f"Unknown account type: {account_type}")

        if not isinstance(order_side, Market):
            raise AccountException(f"Unknown order side: {order_side}")

        def identify_order(orders: List[Order]) -> Union[Order, None]:
            for order in orders:
                if order.order_side == order_side and amount == order.quantity and order.status == OrderStatus.FILLED:
                    return order
            return None

        instrument = get_instrument(self.symphony_client.instruments, instrument_or_symbol)

        if account_type == AccountType.SPOT:
            spot_orders = self.__sub_client.get_all_orders(symbol=instrument.symbol, limit=20)
            spot_orders = [order_from_binance_api(order, instrument, account_type) for order in spot_orders]
            return identify_order(spot_orders)

        if account_type == AccountType.MARGIN:
            margin_orders = self.__sub_client.get_all_margin_orders(symbol=instrument.symbol, limit=20)
            margin_orders = [order_from_binance_api(order, instrument, account_type) for order in margin_orders]
            return identify_order(margin_orders)

        if account_type == AccountType.ISOLATED_MARGIN:
            isolated_margin_orders = self.__sub_client.get_all_margin_orders(symbol=instrument.symbol, limit=20, isIsolated='TRUE')
            isolated_margin_orders = [order_from_binance_api(order, instrument, account_type) for order in isolated_margin_orders]
            return identify_order(isolated_margin_orders)

        return None


    def get_balance(self, account_type: AccountType,
                    balance_type: BalanceType,
                    asset: str,
                    symbol_or_instrument: Optional[Union[str, Instrument]] = None,
                    use_api: Optional[bool] = False
                    ) -> float:
        """
        Fetch a balance. If isolated margin, then symbol must be defined.

        :param account_type: AccountType
        :param balance_type: BalanceType, one of [FREE, LOCKED, BORROWED, INTEREST, NET, NET_BTC]
        :param asset: Asset name
        :param symbol_or_instrument: Symbol or instrument for isolated margin
        :return: The balance as a float
        :raises AccountException: If account type unknown, if balance type unknown, if balance type not valid for account,
                                    If asset not present, if symbol not present
        """

        if account_type not in AccountType:
            raise AccountException(f"Unknown account type: {account_type}")

        if account_type == AccountType.SPOT:
            if isinstance(balance_type, BalanceType) and balance_type in [BalanceType.FREE, BalanceType.LOCKED]:
                # If use api
                if use_api:
                    self.__set_spot_and_initial_account_info()

                # Otherwise
                if asset in self.spot_balances.keys():
                    return self.spot_balances[asset][balance_type]
                else:
                    raise AccountException(f"Asset {asset} not present in SPOT balances")
            else:
                raise AccountException(f"Invalid balance type for SPOT: {balance_type}")

        elif account_type == AccountType.MARGIN:
            if isinstance(balance_type, BalanceType) \
                    and balance_type in [BalanceType.FREE, BalanceType.LOCKED, BalanceType.BORROWED,
                                         BalanceType.INTEREST, BalanceType.NET]:
                # If use api
                if use_api:
                    self.__set_margin_balances_and_account()

                # Otherwise
                if asset in self.margin_balances.keys():
                    return self.margin_balances[asset][balance_type]
                else:
                    raise AccountException(f"Asset {asset} not present in MARGIN balances")
            else:
                raise AccountException(f"Invalid balance type for MARGIN: {balance_type}")
        else:
            if not symbol_or_instrument:
                raise AccountException(f"Symbol must be defined for ISOLATED MARGIN")

            instrument = get_instrument(self.symphony_client.instruments, symbol_or_instrument)
            symbol = instrument.symbol

            if isinstance(balance_type, BalanceType) \
                    and balance_type in [BalanceType.FREE, BalanceType.LOCKED, BalanceType.BORROWED,
                                         BalanceType.INTEREST, BalanceType.NET, BalanceType.NET_BTC]:
                if use_api:
                    self.__set_isolated_margin_balances()

                if symbol in self.isolated_margin_balances.keys():
                    if asset in self.isolated_margin_balances[symbol].keys():
                        return self.isolated_margin_balances[symbol][asset][balance_type]
                    else:
                        raise AccountException(f"Asset {asset} not in isolated margin balances")
                else:
                    raise AccountException(f"Symbol {symbol} not in isolated margin balances")
            else:
                raise AccountException(f"Invalid balance type for ISOLATED_MARGIN: {balance_type}")

    def assets_with_free_balance(self, account_type: AccountType) -> List[str]:
        """
        Return assets, or symbols for isolated margin, with a non-zero BalanceType.FREE

        :param account_type: Account type to query
        :return: List of assets, or list of symbols for ISOLATED_MARGIN
        :raises AccountException: If account type unknown
        """
        if not isinstance(account_type, AccountType):
            raise AccountException(f"Unknown account type: {account_type}")

        if account_type == AccountType.SPOT:
            return [asset for asset in self.spot_balances.keys() if self.spot_balances[asset][BalanceType.FREE] > 0]
        elif account_type == AccountType.MARGIN:
            return [asset for asset in self.margin_balances.keys() if self.margin_balances[asset][BalanceType.FREE] > 0]
        else:
            symbols: List[str] = []
            for symbol in self.isolated_margin_balances.keys():
                for asset in self.isolated_margin_balances[symbol].keys():
                    if self.isolated_margin_balances[symbol][asset][BalanceType.FREE] > 0:
                        symbols.append(symbol)
                        break
            return symbols


    def get_margin_level(self,
                         account_type: AccountType,
                         symbol_or_instrument: Optional[Union[str, Instrument]] = None
                         ) -> float:
        """
        Gets margin level for cross- or isolated margin account. If isolated, symbol or instrument required

        :param account_type: AccountType.MARGIN or AccountType.ISOLATED_MARGIN
        :param symbol_or_instrument: Symbol or instrument
        :return: The margin level
        """
        if account_type not in [AccountType.MARGIN, AccountType.ISOLATED_MARGIN]:
            raise AccountException(f"Unknown account type: {account_type}")
        if account_type == AccountType.MARGIN:
            return self.margin_account.margin_level
        else:
            if not symbol_or_instrument:
                raise AccountException(f"Symbol must be defined if requesting isolated margin level")
            symbol = get_symbol(symbol_or_instrument)
            return self.isolated_margin_levels[symbol]

    def get_margin_ratio(self) -> int:
        """
        Get Cross-Margin margin ratio

        :return: Margin ratio
        """
        return self.margin_account.margin_ratio

    def get_isolated_margin_ratio(self, symbol_or_instrument: Union[str, Instrument]) -> int:
        """
        Get the isolated margin ratio for a particular pair

        :param symbol_or_instrument: Symbol or instrument
        :return: The margin ratio
        """
        return get_instrument(self.symphony_client.instruments, symbol_or_instrument).isolated_margin_ratio

    def create_isolated_margin_socket(self,
                                      symbols_or_instruments: Union[str, Instrument, List[str], List[Instrument]],
                                      create_isolated_margin_account: Optional[bool] = True
                                      ) -> None:
        """
        Creates an isolated margin listener

        :param symbols_or_instruments: The pair or list of pairs to listen on
        :param create_isolated_margin_account: Optionally create isolated margin account if it does not exist
        :return: None
        """
        symbols = self.__extract_pairs(symbols_or_instruments)

        for symbol in symbols:
            if symbol in self.__isolated_margin_conn_keys.keys():
                continue
            if symbol not in self.isolated_margin_pairs:
                raise AccountException(f"{symbol} is not a valid isolated margin pair")
            instrument = get_instrument(self.symphony_client.instruments, symbol)
            sc = self.__sub_client

            #resp = self.__sub_client.get_isolated_margin_account(symbols=symbol)
            if not instrument.isolated_margin_account_created and create_isolated_margin_account:
                logger.info(f"Creating isolated margin account for {symbol}")
                iso_symbol = [iso_symbol for iso_symbol in sc.get_all_isolated_margin_symbols() if iso_symbol["symbol"] == symbol][0]
                self.__create_isolated_margin_account(iso_symbol)
                sleep(0.5)

            resp = self.__sub_client.get_isolated_margin_account(symbols=symbol)
            balances = resp["assets"][0]
            self.__set_isolated_margin_balance(symbol, balances["baseAsset"])

            # Set up websocket
            func = self.__function_template(symbol)
            self.__isolated_margin_conn_keys[symbol] = self.__socket_manager.start_isolated_margin_socket(func, symbol)
            if symbol not in self.__user_isolated_margin_pairs:
                self.__user_isolated_margin_pairs.append(symbol)

            sleep(2.0)
        return

    def stop_isolated_margin_socket(self,
                                    symbols_or_instruments: Union[str, Instrument, List[str], List[Instrument]]
                                    ) -> None:
        """
        Stops an isolated margin listener

        :param symbols_or_instruments: The pair or list of pairs to stop listening on
        :return: None
        """

        symbols = self.__extract_pairs(symbols_or_instruments)
        for symbol in symbols:
            if symbol in self.__isolated_margin_conn_keys.keys():
                self.__socket_manager.stop_socket(self.__isolated_margin_conn_keys[symbol])
                del self.__isolated_margin_conn_keys[symbol]
        return

    def stop(self) -> None:
        """
        Stops the account manager. WARNING: Stops reactor loop, which cannot be restarted. Can only be called ONCE
        per program run

        :return: None
        """
        self.__stop_all()

    def total_free_account_value(self,
                                 denomination: str,
                                 quoter: BinanceRealTimeQuoter,
                                 allowed_spot_assets: Optional[Union[str, List[str]]] = "",
                                 allowed_margin_assets: Optional[Union[str, List[str]]] = "",
                                 allowed_isolated_margin_symbols_or_instruments: Optional[
                                     Union[str, Instrument, List[str], List[Instrument]]] = ""
                                 ) -> float:
        """
        Returns net account value in terms of denomination. Must supply quoter. Optionally allow only certain
        assets or symbols to be included in calculation

        :param denomination: Total balance in terms of
        :param quoter: A BinanceRealTimeQuoter
        :param allowed_spot_assets: Optional spot assets to include, defaults to all
        :param allowed_margin_assets: Optional margin assets to include, defaults to all
        :param allowed_isolated_margin_symbols_or_instruments: Optional isolated margin symbols to include, defaults to all
        :return: Balance
        :raises AccountException: If any asset invalid, if isolated margin symbol is not valid
        """
        all_assets = self.symphony_client.get_all_assets()
        if denomination not in all_assets:
            raise AccountException(f"Denomination {denomination} is not a valid asset")

        conv_chain = ConversionChain(quoter)
        total_balance: float = 0.0
        spot_assets = self.__extract_assets(allowed_spot_assets) if allowed_spot_assets else []
        margin_assets = self.__extract_assets(allowed_margin_assets) if allowed_margin_assets else []
        isolated_margin_symbols = self.__extract_pairs(allowed_isolated_margin_symbols_or_instruments) \
            if allowed_isolated_margin_symbols_or_instruments else []
        for symbol in isolated_margin_symbols:
            if symbol not in self.__isolated_margin_pairs:
                raise AccountException(f"{symbol} is not an isolated margin symbol")

        if spot_assets:
            for spot_asset in spot_assets:
                balance = self.get_balance(AccountType.SPOT, BalanceType.FREE, spot_asset)
                if not balance:
                    continue
                if spot_asset == denomination:
                    total_balance += balance
                else:
                    total_balance += conv_chain.convert(spot_asset, denomination, amount=balance)
        else:
            for asset in self.spot_balances.keys():

                if asset in quoter.symphony_client.non_tradeable_assets:
                    continue
                if asset.startswith("LD"):
                    continue
                balance = self.spot_balances[asset][BalanceType.FREE]

                if not balance:
                    continue
                if asset == denomination:
                    total_balance += balance
                else:
                    total_balance += conv_chain.convert(asset, denomination, amount=balance)

        if margin_assets:
            for margin_asset in margin_assets:
                balance = self.get_balance(AccountType.MARGIN, BalanceType.NET, margin_asset)
                if not balance:
                    continue
                if margin_asset == denomination:
                    total_balance += balance
                else:
                    total_balance += conv_chain.convert(margin_asset, denomination, amount=balance)
        else:
            btc_balance = float(self.__sub_client.get_margin_account()["totalNetAssetOfBtc"])
            if denomination == "BTC":
                total_balance += btc_balance
            else:
                total_balance += conv_chain.convert("BTC", denomination, amount=btc_balance)

        if isolated_margin_symbols:
            for iso_symbol in isolated_margin_symbols:
                if (isolated_margin_symbols and iso_symbol in isolated_margin_symbols) or (not isolated_margin_symbols):
                    for asset in self.isolated_margin_balances[iso_symbol].keys():
                        net_btc_balance = self.isolated_margin_balances[iso_symbol][asset][BalanceType.NET_BTC]
                        if not net_btc_balance:
                            continue
                        if denomination == "BTC":
                            total_balance += net_btc_balance
                        else:
                            total_balance += conv_chain.convert("BTC", denomination, amount=net_btc_balance)

        return total_balance

    def register_order_callback(self, callback: Callable[[Order], None]) -> None:
        """
        Registers a function to pass each order to

        :param callback: The function
        :return: None
        """
        if not callable(callback):
            raise AccountException("Parameter is not callable")
        self.__order_callbacks.append(callback)
        return

    def deregister_order_callback(self, callback: Callable[[Order], None]) -> None:
        """
        Registers a function to pass each order to

        :param callback: The function
        :return: None
        """

        if not callable(callback):
            raise AccountException("Parameter is not callable")
        self.__order_callbacks.remove(callback)
        return

    def register_balance_update_callback(self, callback: Callable[[Order], None]) -> None:
        """
        Registers a function to pass each balance update to

        :param callback: The function
        :return: None
        """
        if not callable(callback):
            raise AccountException("Parameter is not callable")
        self.__balance_update_callbacks.append(callback)
        return

    def deregister_balance_update_callback(self, callback: Callable[[Order], None]) -> None:
        """
        Registers a function to pass each order to

        :param callback: The function
        :return: None
        """

        if not callable(callback):
            raise AccountException("Parameter is not callable")
        self.__balance_update_callbacks.remove(callback)
        return

    def get_transfer_time(self, from_account: AccountType, to_account: AccountType, asset: str, amount: float, transaction_id: int) -> pd.Timestamp:
        """
        Attempts to find transfer of asset between SPOT and MARGIN. Returns most recent timestamp if found

        :param from_account: Origin account
        :param to_account: Destination account
        :param asset: Asset
        :param amount: Amount
        :param transaction_id: Transaction identifier
        :return: Timestamp of transfer
        """
        if from_account == AccountType.ISOLATED_MARGIN or to_account == AccountType.ISOLATED_MARGIN:
            raise AccountException(f"Cannot retrieve transfer history for isolated margin accounts")
        if from_account == AccountType.SPOT and to_account == AccountType.MARGIN:
            transfer_type = "MAIN_MARGIN"
        elif from_account == AccountType.MARGIN and to_account == AccountType.SPOT:
            transfer_type = "MARGIN_MAIN"
        else:
            raise AccountException(f"Unimplemented transfer history request for {from_account} to {to_account}")
        resp = self.__sub_client.query_universal_transfer_history(type=transfer_type, size=20)
        for row in resp['rows']:
            if row['asset'] == asset and float(row['amount']) == amount and int(row['tranId']) == transaction_id:
                return pd.Timestamp(row['timestamp'], unit='ms', tz='UTC')
        raise AccountException(f"Could not find transfer {from_account} to {to_account} of {amount} {asset}")

    def __set_spot_and_initial_account_info(self) -> None:
        """
        Sets the initial account info (e.g. commission rates) and spot balances

        :return: None
        """
        account_info = self.__sub_client.get_account()
        self.maker_commission = account_info["makerCommission"] / 10000
        self.taker_commission = account_info["takerCommission"] / 10000
        for balance in account_info["balances"]:
            asset = balance["asset"]
            if asset not in self.spot_balances.keys():
                self.spot_balances[asset] = {}
            self.spot_balances[asset][BalanceType.FREE] = float(balance["free"])
            self.spot_balances[asset][BalanceType.LOCKED] = float(balance["locked"])
            if asset == "BNB" and self.spot_balances[asset][BalanceType.FREE] > 0:
                self.maker_commission = self.maker_commission * 0.75
                self.taker_commission = self.taker_commission * 0.75

    def __set_open_spot_orders(self) -> None:
        """
        Fetches all open orders, converts to Order objects, and stores

        :return: None
        """
        orders = self.__sub_client.get_open_orders()
        for order in orders:
            new_order = self.__build_order(order)
            if new_order not in self.open_spot_orders:
                self.open_spot_orders.append(new_order)
        return

    def __set_margin_balances_and_account(self) -> None:
        margin_info = self.__sub_client.get_margin_account()
        self.margin_account = self.__get_margin_account(margin_info)
        for balance in margin_info["userAssets"]:
            asset = balance['asset']
            if asset not in self.margin_balances.keys():
                self.margin_balances[asset] = {}

            self.margin_balances[asset][BalanceType.FREE] = float(balance['free'])
            self.margin_balances[asset][BalanceType.LOCKED] = float(balance['locked'])
            self.margin_balances[asset][BalanceType.BORROWED] = float(balance['borrowed'])
            self.margin_balances[asset][BalanceType.INTEREST] = float(balance['interest'])
            self.margin_balances[asset][BalanceType.NET] = float(balance['netAsset'])
        return

    def __get_margin_account(self, get_margin_account_resp: Optional[Dict] = None) -> MarginAccount:
        """
        Builds the MarginAccount object

        :param get_margin_account_resp: Accepts api response
        :return: new MarginAccount
        """
        if not get_margin_account_resp:
            get_margin_account_resp = self.__sub_client.get_margin_account()

        margin_account = MarginAccount()
        margin_account.trade_enabled = get_margin_account_resp['tradeEnabled']
        margin_account.transfer_enabled = get_margin_account_resp['transferEnabled']
        margin_account.borrow_enabled = get_margin_account_resp['borrowEnabled']
        margin_account.margin_level = float(get_margin_account_resp['marginLevel'])
        margin_account.margin_ratio = 3
        margin_account.total_asset_denomination = 'BTC'
        margin_account.total_assets = float(get_margin_account_resp['totalAssetOfBtc'])
        margin_account.total_liability = float(get_margin_account_resp['totalLiabilityOfBtc'])
        margin_account.total_net = float(get_margin_account_resp['totalNetAssetOfBtc'])
        return margin_account

    def __set_isolated_margin_balances(self, create_isolated_margin_account: Optional[bool] = False) -> None:
        """
        Set known balances for isolated margin. These are keyed to symbol

        :param create_isolated_margin_account: Optionally create isolated margin accounts if they don't exist
        :return: None
        """
        balances = self.__sub_client.get_isolated_margin_account()
        all_symbols = self.__sub_client.get_all_isolated_margin_symbols()

        for asset in balances["assets"]:
            symbol = asset["symbol"]
            self.__isolated_margin_pairs.append(symbol)
            base_asset = asset["baseAsset"]
            quote_asset = asset["quoteAsset"]
            self.__set_isolated_margin_balance(symbol, base_asset)
            self.__set_isolated_margin_balance(symbol, quote_asset)

        for iso_symbol in all_symbols:
            symbol = iso_symbol["symbol"]
            # Already have account open and info populated
            if symbol in self.isolated_margin_balances.keys():
                continue
            # Optionally initialize account
            if create_isolated_margin_account:
                if not self.__create_isolated_margin_account(iso_symbol):
                    continue

            self.__set_isolated_margin_balance(symbol, iso_symbol["base"], zero_fill=True)
            self.__set_isolated_margin_balance(symbol, iso_symbol["quote"], zero_fill=True)

        return

    def __set_isolated_margin_balance(self, symbol: str, asset: Dict[str, str], zero_fill: Optional[bool] = False) -> None:
        """
        Sets an isolated margin balance

        :param symbol: Symbol name
        :param asset: Asset object from API
        :param zero_fill: Whether or not to zerofill balance
        :return: None
        """

        if not zero_fill:
            asset_name = asset["asset"]
        else:
            asset_name = asset

        if symbol not in self.isolated_margin_balances.keys():
            self.isolated_margin_balances[symbol] = {}
        if asset_name not in self.isolated_margin_balances[symbol].keys():
            self.isolated_margin_balances[symbol][asset_name] = {}
        self.isolated_margin_balances[symbol][asset_name][BalanceType.BORROWED] = 0.0 if zero_fill else float(
            asset["borrowed"])
        self.isolated_margin_balances[symbol][asset_name][BalanceType.FREE] = 0.0 if zero_fill else float(asset["free"])
        self.isolated_margin_balances[symbol][asset_name][BalanceType.INTEREST] = 0.0 if zero_fill else float(
            asset["interest"])
        self.isolated_margin_balances[symbol][asset_name][BalanceType.LOCKED] = 0.0 if zero_fill else float(
            asset["locked"])
        self.isolated_margin_balances[symbol][asset_name][BalanceType.NET] = 0.0 if zero_fill else float(
            asset["netAsset"])
        self.isolated_margin_balances[symbol][asset_name][BalanceType.NET_BTC] = 0.0 if zero_fill else float(
            asset["netAssetOfBtc"])
        return

    def __create_isolated_margin_account(self, iso_symbol_obj: Dict[str, str]) -> bool:
        """
        Creates isolated margin account for a symbol

        :param iso_symbol_obj: The response from the API
        :return: True if account created, false otherwise
        """
        symbol = iso_symbol_obj["symbol"]
        instrument = get_instrument(self.symphony_client.instruments, symbol)
        try:
            resp = self.__sub_client.create_isolated_margin_account(base=iso_symbol_obj["base"],
                                                                    quote=iso_symbol_obj["quote"])
            breakpoint()
            instrument.isolated_margin_account_created = True
            base_asset = iso_symbol_obj["base"]
            quote_asset = iso_symbol_obj["quote"]
            self.isolated_margin_balances[symbol] = {}
            self.isolated_margin_balances[symbol][base_asset] = {}
            self.isolated_margin_balances[symbol][quote_asset] = {}

            def zero(asset: str) -> None:
                self.isolated_margin_balances[symbol][asset][BalanceType.BORROWED] = 0.0
                self.isolated_margin_balances[symbol][asset][BalanceType.FREE] = 0.0
                self.isolated_margin_balances[symbol][asset][BalanceType.INTEREST] = 0.0
                self.isolated_margin_balances[symbol][asset][BalanceType.LOCKED] = 0.0
                self.isolated_margin_balances[symbol][asset][BalanceType.NET] = 0.0
                self.isolated_margin_balances[symbol][asset][BalanceType.NET_BTC] = 0.0

            zero(base_asset)
            zero(quote_asset)

            # Get margin ratio
            assets = self.__sub_client.get_isolated_margin_account()
            for asset in assets["assets"]:
                if asset["symbol"] == symbol:
                    instrument.isolated_margin_ratio = int(asset['marginRatio'])
                    break
            if not instrument.isolated_margin_ratio:
                raise AccountException(f"Could not fetch isolated margin ratio after creating account for {symbol}")


        except BinanceAPIException as e:
            if "Pair not found" in str(e):
                logger.info(f"Symbol [{symbol}] not a tradeable isolated margin symbol. Skipping.")
            elif "delisted" in str(e):
                logger.info(f"Symbol [{symbol}] will soon be delisted from isolated margin. Skipping.")
            return False
        return True

    def __set_historical_orders(self, write_orders: Optional[bool] = True) -> None:
        """
        Fetches historical orders.

        :param write_orders: Optionally write orders to DynamoDB
        :return: None
        :raises Exception: For all exceptions
        """
        ccxt_client = self.symphony_client.ccxt_client

        models = []

        def append_order(account_type: AccountType, derived_order: Order) -> None:
            if derived_order.status == OrderStatus.CANCELLED:
                return
            if account_type == AccountType.SPOT and derived_order not in self.filled_spot_orders:
                self.filled_spot_orders.append(derived_order)
            elif account_type == AccountType.MARGIN and derived_order not in self.filled_margin_orders:
                self.filled_margin_orders.append(derived_order)
            elif account_type == AccountType.ISOLATED_MARGIN and derived_order not in self.filled_isolated_margin_orders:
                self.filled_isolated_margin_orders.append(derived_order)
            if write_orders:
                models.append(order_model_from_order(derived_order))
            logger.info(f"Found historical order: {repr(derived_order)}")
            return

        # Fetch SPOT
        for spot_instrument in self.symphony_client.instruments:
            symbol = spot_instrument.base_asset + "/" + spot_instrument.quote_asset
            try:
                orders = ccxt_client.fetchClosedOrders(symbol=symbol)
                for order_obj in orders:
                    order = order_from_cctx(order_obj, Exchange.BINANCE, spot_instrument, AccountType.SPOT)
                    append_order(AccountType.SPOT, order)
            except BadSymbol:
                pass
            except Exception as e:
                raise e

        # Fetch Margin
        margin_instruments: List[Instrument] = [instrument for instrument in self.symphony_client.instruments if
                                                instrument.margin_allowed]
        for margin_instrument in margin_instruments:
            symbol = margin_instrument.base_asset + "/" + margin_instrument.quote_asset
            margin_orders = ccxt_client.fetchClosedOrders(symbol=symbol, params={'type': 'margin'})
            sleep(1)
            for margin_order in margin_orders:
                order = order_from_cctx(margin_order, Exchange.BINANCE, margin_instrument, AccountType.MARGIN)
                append_order(AccountType.MARGIN, order)

        iso_margin_instruments: List[Instrument] = [instrument for instrument in self.symphony_client.instruments if
                                                    instrument.isolated_margin_allowed]
        for iso_margin_instrument in iso_margin_instruments:
            symbol = iso_margin_instrument.base_asset + "/" + iso_margin_instrument.quote_asset
            try:
                iso_margin_orders = ccxt_client.fetchClosedOrders(symbol=symbol,
                                                                  params={'type': 'margin', 'isIsolated': 'TRUE'})
                sleep(1)
                for iso_margin_order in iso_margin_orders:
                    order = order_from_cctx(iso_margin_order, Exchange.BINANCE, iso_margin_instrument,
                                            AccountType.ISOLATED_MARGIN)
                    append_order(AccountType.ISOLATED_MARGIN, order)
            except ExchangeError as e:
                if "ISOLATED_MARGIN_ACCOUNT_NO_OPENED" in str(e):
                    continue
                else:
                    raise e

        if write_orders:
            for model in models:
                insert_or_update_order(model)
            all_orders = [order for order in models]
            logger.info(f"Wrote {len(all_orders)} to DynamoDB")
        return

    def __build_order(self, order: Dict, account_type: AccountType, source: Optional[str] = "API") -> Order:
        """
        Builds an Order object from API response

        :param order: The API response
        :param account_type: Account type
        :param source: One of ["API", "MSG", "CCTX"]
        :return: new Order
        """
        if source.upper() not in ["API", "MSG", "CCTX"]:
            raise AccountException(f"Unknown message source: {source}")
        if source.upper() == "API" or source.upper() == "CCXT":
            symbol = order["symbol"]
        if source.upper() == "MSG":
            symbol = order["s"]

        instrument = get_instrument(self.symphony_client.get_all_instruments(), symbol)
        if source.upper() == "API":
            return order_from_binance_api(order, instrument, account_type)
        elif source.upper() == "CCTX":
            return order_from_cctx(order, Exchange.BINANCE, instrument, account_type)
        else:
            return order_from_binance_websocket(order, instrument, account_type)

    def __update_margin_level(self, account_type: AccountType, symbol: Optional[str]) -> None:
        """
        Update the margin level for either cross-margin or isolated margin

        :param account_type: Either isolated or cross-margin
        :param symbol: Needed if isolated margin
        :return: None
        """
        if account_type == AccountType.MARGIN:
            resp = self.__sub_client.get_margin_account()
            self.margin_account.margin_level = float(resp["marginLevel"])
        elif account_type == AccountType.ISOLATED_MARGIN:
            resp = self.__sub_client.get_isolated_margin_account(symbols=symbol)
            self.isolated_margin_levels[symbol] = float(resp['assets'][0]['marginLevel'])
        return

    def __handle_spot_event(self, msg: Dict) -> None:
        """
        Handles spot trading websocket message

        :param msg: The websocket message
        :return: None
        :raises AccountException: If unknown message type
        """
        self.__dispatch_event(msg, AccountType.SPOT)
        return

    def __handle_cross_margin_event(self, msg: Dict) -> None:
        """
        Handles Cross-Margin websocket message

        :param msg: The websocket message
        :return: None
        :raises AccountException: If unknown message type
        """
        self.__dispatch_event(msg, AccountType.MARGIN)
        pass

    def __handle_isolated_margin_event(self, msg: Dict, symbol: str) -> None:
        """
        Handles Isolated Margin websocket message

        :param msg: The websocket message
        :return: None
        :raises AccountException: If unknown message type
        """
        self.__dispatch_event(msg, AccountType.ISOLATED_MARGIN, symbol=symbol)
        pass

    def __dispatch_event(self, msg: Dict, account_type: AccountType, symbol: str = "") -> None:
        """
        All events share the same structure, this routes events with correct account type

        :param msg: The websocket message
        :param account_type: Account type
        :return: None
        """
        event_type = msg["e"]
        if event_type == "error":
            logger.error(f"WebSocket error for {account_type}{'-' + symbol if symbol else ''}. Msg: {msg['m']}")
            return
        if event_type == "executionReport":
            self.__handle_execution_report(msg, account_type)
        elif event_type == "outboundAccountPosition":
            self.__handle_outbound_account_position(msg, account_type, symbol=symbol)
        elif event_type == "balanceUpdate":
            self.__handle_balance_update(msg, account_type, symbol=symbol)
        elif event_type == "listStatus":
            logger.info(f"Not yet implemented listStatus, {msg}")
            pass
        else:
            raise AccountException(f"Unknown spot message! {msg}, Account type: {account_type.value}")
        return

    def __handle_execution_report(self, report, account_type: AccountType) -> None:
        """
        Handles executionReport, i.e. a trade event. Adds to open_spot_orders, or moves order to closed_spot_orders.
        Optionally also writes Order to configured database

        :param report: The message
        :param account_type: Account type
        :return: None
        :raises AccountException: If duplicate orders found
        """
        order_id = int(report["i"])
        symbol = str(report["s"])
        order_status = OrderStatus.get_status(report["X"])
        matching_orders = [order for order in self.open_spot_orders if order.order_id == order_id]

        order_for_callbacks = self.__build_order(report, account_type, source="MSG")
        self.__fire_order_callbacks(order_for_callbacks)

        if order_status == OrderStatus.OPEN:
            new_order = self.__build_order(report, account_type, source="MSG")
            for order in matching_orders:
                self.__remove_order(order)
            self.__append_order(new_order)
            if self.__write_orders:
                order_model = order_model_from_order(new_order)
                insert_or_update_order(order_model)
            logger.info(f" [{account_type.value.upper()}][CREATED] -> {repr(new_order)}")
        elif order_status == OrderStatus.FILLED:
            if not len(matching_orders):
                new_order = self.__build_order(report, account_type, source="MSG")
                self.__append_order(new_order)
            elif len(matching_orders) == 1:
                new_order = matching_orders[0]
                self.__remove_order(new_order)
                new_order.status = OrderStatus.FILLED
                new_order.price = float(report["L"])
                new_order.commission_amount = float(report["n"])
                new_order.commission_asset = None if isinstance(report["N"], type(None)) else str(report["N"])
                self.__append_order(new_order)
                if self.__write_orders:
                    order_model = order_model_from_order(new_order)
                    insert_or_update_order(order_model)
            else:
                raise AccountException(f"Found multiple matching orders for {report}")

            logger.info(f" [{account_type.value.upper()}][FILLED] -> {repr(new_order)}")
        elif order_status == OrderStatus.CANCELLED:
            if matching_orders:
                for order in matching_orders:
                    order.status = OrderStatus.CANCELLED
                    self.__remove_order(order)
                    if self.__write_orders:
                        insert_or_update_order(order)
                    logger.info(f" [{account_type.value.upper()}][CANCELLED] -> {repr(order)}")
        if account_type == AccountType.MARGIN:
            self.__update_margin_level(AccountType.MARGIN)
        elif account_type == AccountType.ISOLATED_MARGIN:
            self.__update_margin_level(AccountType.ISOLATED_MARGIN, symbol=symbol)
        return

    def __append_order(self, order: Order) -> None:
        """
        Appends order to the correct array

        :param order: The order
        :return: None
        :raises AccountException: If the account type is unknown
        """
        if order.account == AccountType.SPOT:
            self.open_spot_orders.append(order)
        elif order.account == AccountType.MARGIN:
            self.open_margin_orders.append(order)
        elif order.account == AccountType.ISOLATED_MARGIN:
            self.open_isolated_margin_orders.append(order)
        else:
            raise AccountException(f"Unknown account type: {order.account}")
        return

    def __remove_order(self, order: Order) -> None:
        """
        Removes order from the correct array

        :param order: The order
        :return: None
        :raises AccountException: If the account type is unknown
        """
        if order.account == AccountType.SPOT:
            self.open_spot_orders.remove(order)
        elif order.account == AccountType.MARGIN:
            self.open_margin_orders.remove(order)
        elif order.account == AccountType.ISOLATED_MARGIN:
            self.open_isolated_margin_orders.remove(order)
        else:
            raise AccountException(f"Unknown account type: {order.account}")
        return

    def __fire_order_callbacks(self, order: Order) -> None:
        """
        Launches the order callbacks

        :param order: The new order
        :return: None
        """
        for callback in self.__order_callbacks:
            callback(order)
        return

    def __handle_outbound_account_position(self, report, account_type: AccountType, symbol: str = "") -> None:
        """
        Handles live changes to account balance

        :param report: WebSocket message
        :param account_type: Account type
        :param symbol: Optional symbol for isolated margin
        :return: None
        """
        for balance in report["B"]:
            asset = balance['a']
            if account_type == AccountType.SPOT:
                self.spot_balances[asset][BalanceType.FREE] = float(balance['f'])
                self.spot_balances[asset][BalanceType.LOCKED] = float(balance["l"])
            elif account_type == AccountType.MARGIN:
                self.margin_balances[asset][BalanceType.FREE] = float(balance['f'])
                self.margin_balances[asset][BalanceType.LOCKED] = float(balance["l"])
            elif account_type == AccountType.ISOLATED_MARGIN:
                if asset not in self.isolated_margin_balances[symbol].keys():
                    self.isolated_margin_balances[symbol][asset] = {}
                self.isolated_margin_balances[symbol][asset][BalanceType.FREE] = float(balance['f'])
                self.isolated_margin_balances[symbol][asset][BalanceType.LOCKED] = float(balance["l"])
            else:
                raise AccountException(f"Unknown account type: {account_type}")

        return

    def __handle_balance_update(self, report, account_type: AccountType, symbol: str = ""):
        """
        Handles a balance change

        :param report: WebSocket message
        :param account_type: Account type
        :param symbol: Optional symbol for isolated margin
        :raises AccountException: If symbol not provided for isolated margin
        :return:
        """
        asset = report["a"]
        if account_type == AccountType.SPOT:
            if "free" not in self.spot_balances[asset].keys():
                self.spot_balances[asset]["free"] = float(report["d"])
            else:
                self.spot_balances[asset]["free"] += float(report["d"])
        elif account_type == AccountType.MARGIN:
            if "free" not in self.margin_balances[asset].keys():
                self.margin_balances[asset]["free"] = float(report["d"])
            else:
                self.margin_balances[asset]["free"] += float(report["d"])
        elif account_type == AccountType.ISOLATED_MARGIN:
            if not symbol:
                raise AccountException(f"No symbol provided for isolated margin balance update!")
            if "free" not in self.isolated_margin_balances[symbol][asset].keys():
                self.isolated_margin_balances[symbol][asset]["free"] = float(report["d"])
            else:
                self.isolated_margin_balances[symbol][asset]["free"] += float(report["d"])
        else:
            raise AccountException(f"Unknown account type: {account_type}")
        if self.__balance_update_callbacks:
            for callback in self.__balance_update_callbacks:
                callback(report, account_type, symbol=symbol)
        return

    def __function_template(self, symbol):
        def func1(msg):
            self.__handle_isolated_margin_event(msg, symbol)
            return

        func1.__name__ = f"__isolated_margin_socket_handler_{symbol}"
        return func1

    def __extract_pairs(self, symbol_or_instruments: Union[str, Instrument, List[str], List[Instrument]]) -> List[str]:
        """
        Takes either a single instrument or pair or list of either and returns list of strings

        :param symbol_or_instruments: The individual or list of symbols or instruments
        :return: List of pairs as strings
        :raises AccountException: If unacceptable type
        """
        # Standardize user provided isolated margin pairs
        if isinstance(symbol_or_instruments, list):
            if isinstance(symbol_or_instruments[0], str):
                return symbol_or_instruments
            elif isinstance(symbol_or_instruments[0], Instrument):
                return [instrument.symbol for instrument in symbol_or_instruments]
            else:
                raise AccountException(f"Unknown type: {type(symbol_or_instruments[0])}")
        elif isinstance(symbol_or_instruments, str):
            return [symbol_or_instruments]
        elif isinstance(symbol_or_instruments, Instrument):
            return [symbol_or_instruments.symbol]
        else:
            raise AccountException(f"Unknown type: {type(symbol_or_instruments)}")

    def __extract_assets(self, assets: Union[str, List[str]]) -> List[str]:
        """
        Extract and check assets from a single asset or a list of them.

        :param assets: Single or list of assets
        :return: List of assets, even if only one
        :raises AccountException: If asset unknown
        """
        all_assets = self.symphony_client.get_all_assets()
        if isinstance(assets, str):
            if assets not in all_assets:
                raise AccountException(f"Unknown asset: {assets}")
            return [assets]
        elif isinstance(assets, list):
            ats = []
            for p in assets:
                if p not in all_assets:
                    raise AccountException(f"Unknown asset: {p}")
                ats.append(p)
            return ats
        else:
            raise AccountException(f"Unknown type: {type(assets)}")

    def __stop_all(self) -> None:
        """
        This can only be called once!

        :return: None
        """
        self.__socket_manager.close()
        return
