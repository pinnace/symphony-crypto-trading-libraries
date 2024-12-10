from symphony.enum import Market, Exchange
from symphony.abc import RealTimeQuoter, HistoricalQuoter
from symphony.exceptions import DataClassException, UtilsException
from .instrument import Instrument, filter_instruments
from typing import Union, List, Final, Optional
from symphony.utils.graph import bidirectional_conversion_chain, build_graph, verify_chain, \
    CurrencyConversionGraph, ConversionChainType, get_execution_chain, get_instrument_chain, shortest_conversion_chains, \
    find_shortest_path, path_to_conversion_chain
from symphony.utils.instruments import get_instrument
from dataclasses import dataclass
from functools import lru_cache


@dataclass
class ConversionChain:
    """
    Manages fetching the conversion chain. One quoter per.
    """

    def __init__(self,
                 quoter: Union[RealTimeQuoter, HistoricalQuoter],
                 start_asset: Optional[str] = "",
                 target_instrument: Optional[Instrument] = None,
                 order_type: Optional[Market] = Market.BUY,
                 highest_liquidity_chain: Optional[bool] = False):
        """
        Initialize the ConversionChain

        :param quoter: A quoter, either RealTime or Historical
        :param start_asset: The starting asset (e.g. 'EUR', 'USDT')
        :param target_instrument: First target instrument
        :param order_type: First order type
        :param highest_liquidity_chain: Whether to attempt to find the highest liquidity chain
        :raises DataClassException: If interface not implemented, start_asset invalid
        """

        self.symphony_client = None
        self.__quoter_set = False
        self.quoter: Final[Union[RealTimeQuoter, HistoricalQuoter]] = quoter
        if isinstance(self.quoter, HistoricalQuoter):
            raise DataClassException(f"Not implemented historical quoter")

        self.instruments: List[Instrument] = self.quoter.instruments
        self.target_instrument: Instrument = target_instrument
        self.order_type = order_type
        self.graph = build_graph(self.instruments)
        self.highest_liquidity_chain = highest_liquidity_chain
        self.conversion_chain: ConversionChainType = None
        self.instrument_chain = None
        self.execution_chain = None
        self.start_asset: str = start_asset
        self.all_assets: List[str] = []

        if not self.start_asset:
            return

        if not ConversionChain.verify_start_asset(self.start_asset, self.instruments):
            raise DataClassException(f"{start_asset} is not a valid start asset for this exchange (has no pairs)")

        if not isinstance(target_instrument, Instrument):
            raise DataClassException(f"target_instrument must be an instrument. Type: {type(target_instrument)}")

        if self.start_asset and self.target_instrument and self.order_type:
            self.set_chain(self.target_instrument, start_asset=self.start_asset,
                           order_type=self.order_type, highest_liquidity_chain=highest_liquidity_chain)
        return

    @property
    def conversion_chain(self) -> ConversionChainType:
        return self.__conversion_chain

    @conversion_chain.setter
    def conversion_chain(self, conversion_chain: ConversionChainType):
        self.__conversion_chain = conversion_chain

    @property
    def start_asset(self) -> str:
        return self.__start_asset

    @start_asset.setter
    def start_asset(self, start_asset: str):
        if len(filter_instruments(self.instruments, start_asset)):
            raise DataClassException(f"The asset {start_asset} should not be an Instrument")
        self.__start_asset = start_asset

    @property
    def instrument_chain(self) -> List[Instrument]:
        return self.__instrument_chain

    @instrument_chain.setter
    def instrument_chain(self, instrument_chain: List[Instrument]):
        self.__instrument_chain = instrument_chain

    @property
    def execution_chain(self) -> List[Market]:
        return self.__execution_chain

    @execution_chain.setter
    def execution_chain(self, execution_chain: List[Market]):
        self.__execution_chain = execution_chain

    @property
    def quoter(self) -> Union[RealTimeQuoter, HistoricalQuoter]:
        return self.__quoter

    @quoter.setter
    def quoter(self, quoter: Union[RealTimeQuoter, HistoricalQuoter]):
        if not self.__quoter_set:
            self.__quoter = quoter
            self.__quoter_set = True
            return
        raise DataClassException(f"Quoter is fixed and cannot be reset")

    @property
    def order_type(self) -> Market:
        return self.__order_type

    @order_type.setter
    def order_type(self, order_type: Market):
        if order_type != Market.BUY and order_type != Market.SELL:
            raise DataClassException(f"Order type {order_type} should be {Market.BUY} or {Market.SELL}")
        self.__order_type = order_type

    def set_chain(self,
                  target_instrument_or_asset: Union[Instrument, str],
                  start_asset: Optional[str] = "",
                  order_type: Optional[Market] = Market.BUY,
                  highest_liquidity_chain: Optional[bool] = True) -> ConversionChainType:
        """
        Calculate and verify the conversion chain. Set all instance properties

        :param target_instrument_or_asset: The target Instrument or asset
        :param start_asset: The start asset (e.g. 'EUR', 'BTC')
        :param order_type: The Market order type
        :param highest_liquidity_chain: Attempts to find the chain with the best liquidity level.
                                            Does not work if the quoter is a HistoricalQuoter. Only tries after attempting
                                            to find the shortest chain, and multiple are found.
        :return: The conversion chain.
        :raises DataClassException: If no chains are found, if chains did not verify
        """
        if start_asset:
            self.start_asset = start_asset
        if order_type:
            self.order_type = order_type

        if isinstance(target_instrument_or_asset, Instrument):
            self.target_instrument = target_instrument_or_asset
        elif isinstance(target_instrument_or_asset, str):
            if target_instrument_or_asset in self.quoter.symphony_client.get_all_assets():
                try:
                    # Potentially a symbol that has both assets
                    potential_single: Instrument = self.__get_asset_pair(start_asset, target_instrument_or_asset)
                    if potential_single:
                        potential_single_chain: ConversionChainType = [potential_single.symbol]
                        execution_chain: List[Market] = get_execution_chain(potential_single_chain, self.start_asset, self.instruments)
                        instrument_chain: List[Instrument] = get_instrument_chain(potential_single_chain, self.instruments)
                        self.conversion_chain: ConversionChainType = potential_single_chain
                        self.execution_chain = execution_chain
                        self.instrument_chain = instrument_chain
                        return potential_single_chain
                    # Otherwise find most liquid asset with the target asset
                    self.target_instrument = self.__most_liquid_instrument_for_asset(target_instrument_or_asset)
                except DataClassException:
                    pass
            if not self.target_instrument:
                self.target_instrument = get_instrument(self.instruments, target_instrument_or_asset)
        else:
            raise DataClassException(f"Unknown type for target_instrument_or_asset: {type(target_instrument_or_asset)}")

        chains: List[ConversionChainType] = bidirectional_conversion_chain(self.graph, self.start_asset,
                                                                           self.target_instrument)

        for chain in chains:
            if not verify_chain(chain, self.start_asset, self.instruments):
                raise DataClassException(f"Chain {chain} failed to verify")

        chains: List[ConversionChainType] = shortest_conversion_chains(chains)

        if highest_liquidity_chain and len(chains) > 1 and not isinstance(self.quoter, HistoricalQuoter):
            chain: ConversionChainType = self.__most_liquid_chain(chains)
        elif not len(chains):
            raise DataClassException(f"No chains found for {self.start_asset} and {self.target_instrument.symbol}!")
        else:
            chain: ConversionChainType = chains[0]

        # Final sanity check. See if target is in any intermediate pairs
        if len(chain) > 2:
            for i, symbol in enumerate(chain[:-1]):
                intermediate_instrument = get_instrument(self.instruments, symbol)
                if target_instrument_or_asset == intermediate_instrument.base_asset or target_instrument_or_asset == intermediate_instrument.quote_asset:
                    print(f"Abnormal chain {chain}. Setting chain to {chain[:i + 1]}")
                    chain = chain[:i + 1]
                    break

        execution_chain: List[Market] = get_execution_chain(chain, self.start_asset, self.instruments)
        instrument_chain: List[Instrument] = get_instrument_chain(chain, self.instruments)
        self.conversion_chain: ConversionChainType = chain
        self.execution_chain = execution_chain
        self.instrument_chain = instrument_chain
        return self.conversion_chain

    def min_quantity_for_chain(self, start_asset: str, conversion_chain: ConversionChainType) -> float:
        """
        Returns the minimum quantity needed of the start asset to (probably) successfully execute the chain.

        :param start_asset: Starting asset
        :param conversion_chain: The conversion chain being executed
        :return: The minimum cost basis amount
        """
        ccxt_client = self.quoter.symphony_client.ccxt_client

        max_min_cost = 0.0
        for symbol in conversion_chain:
            instrument = get_instrument(self.instruments, symbol)
            ccxt_symbol = instrument.base_asset + "/" + instrument.quote_asset
            min_base_cost = float(ccxt_client.markets[ccxt_symbol]['limits']['cost']['min'])
            cost_in_start_asset = self.convert(start_asset, instrument.quote_asset, amount=min_base_cost)
            if cost_in_start_asset > max_min_cost:
                max_min_cost = cost_in_start_asset
        return max_min_cost

    def __most_liquid_chain(self, chains: List[ConversionChainType]) -> ConversionChainType:
        """
        Attempts to identify the most liquid conversion chain by taking the average liquidity along the chain
        and returning the chain with the lowest.

        :param chains: Conversion chains
        :return: Best chain
        """
        # TODO: Mock for historical quoter
        best_chain_index = -1
        lowest_liquidity = 0
        for i, chain in enumerate(chains):
            liquidity_total = 0
            for symbol in chain[:-1]:
                liquidity = self.quoter.get_liquidity(symbol, fall_back_to_api=True)
                liquidity_total += liquidity
            liquidity_avg = liquidity_total / len(chain[:-1])
            if not lowest_liquidity or liquidity_avg < lowest_liquidity:
                lowest_liquidity = liquidity_avg
                best_chain_index = i
        return chains[best_chain_index]

    def __most_liquid_instrument_for_asset(self, asset: str) -> Instrument:
        """
        Finds the most liquid pair for a particular asset

        :param asset: Target asset
        :return: Most liquid Instrument
        :raises DataClassException: If asset unknown, if instrument could not be identified
        """
        if asset not in self.get_all_assets():
            raise DataClassException(f"Unknown asset {asset}")

        valid_pairs = [
            instrument for instrument in self.instruments
            if instrument.base_asset == asset or instrument.quote_asset == asset
        ]
        if not valid_pairs:
            raise DataClassException(f"Could not identify liquid instrument for asset {asset}")

        liquidities = [self.quoter.get_liquidity(pair, fall_back_to_api=True) for pair in valid_pairs]
        min_index = liquidities.index(min(liquidities))
        return valid_pairs[min_index]

    @staticmethod
    def verify_start_asset(start_asset: str, all_instruments: List[Instrument]) -> bool:
        """
        Verifies that the asset we have chosen is actually valid

        :return: True or False
        """
        for instrument in all_instruments:
            if start_asset == instrument.base_asset or start_asset == instrument.quote_asset:
                return True
        return False

    def get_all_pairs_with(self, asset: str) -> List[Instrument]:
        """
        Returns a list of instruments that contain a particular asset

        :param asset: The asset to search for
        :return: List of instruments with this asset
        """
        pairs: List[Instrument] = [instrument for instrument in self.instruments
                                   if instrument.base_asset == asset or instrument.quote_asset == asset]
        return pairs

    def get_all_assets(self) -> List[str]:
        """
        Returns all unique assets
        :return:
        """
        if self.all_assets:
            return self.all_assets
        all_assets = []
        for instrument in self.instruments:
            all_assets.append(instrument.base_asset)
            all_assets.append(instrument.quote_asset)
        all_assets = list(set(all_assets))
        return all_assets

    def __get_asset_pair(self, start_asset: str, end_asset: str) -> Union[Instrument, None]:
        """
        Handle edge case for asset -> asset parameters. There may be a single pair that has both assets.

        :param start_asset: Start asset
        :param end_asset: Target asset
        :return: The instrument, or None
        """
        try:
            instrument = get_instrument(self.instruments, start_asset + end_asset)
            return instrument
        except UtilsException:
            pass

        try:
            instrument = get_instrument(self.instruments, end_asset + start_asset)
            return instrument
        except UtilsException:
            pass
        return None

    def convert(self,
                start: str,
                end: str, amount: Optional[float] = 1,
                order_type: Optional[Market] = None,
                fall_back_to_api: Optional[bool] = True,
                ) -> float:
        """
        Converts one arbitrary asset to another (e.g. 'BTC' -> 'BVND', 'ETH' -> 'INJ'). If there is not an already trading
        pair, this converter will create a 'virtual pair' using multiple rates. Because of this, rate
        may not be perfectly synonymous with your dashboard in certain circumstance (e.g. small cap token to exotic
        currency, quoter samples a swing high or swing low bid/ask).

        :param start: The asset to convert
        :param end: The asset we want the rate of
        :param amount: Amount of `start` asset to convert, defaults to unit price [1]
        :param order_type: Market order type. Dictates if we take the Bid or the Ask
        :param fall_back_to_api: For quoter, if we should fall back to the api if no real time quotes available
        :return: The converted amount
        :raises DataClassException:
        """
        if not ConversionChain.verify_start_asset(start, self.instruments):
            #raise DataClassException(f"Asset {start} is not valid")
            return 0.0

        if not amount:
            return 0.0

        potential_single_instrument = self.__get_asset_pair(start, end)
        if potential_single_instrument:
            market_price = self.__get_market_price(potential_single_instrument.symbol, order_type=order_type, fall_back_to_api=fall_back_to_api)
            if potential_single_instrument.quote_asset == end:
                return market_price * amount
            else:
                return amount / market_price

        shortest_path = find_shortest_path(self.graph, start, end)
        if shortest_path:

            shortest_chain = path_to_conversion_chain(shortest_path)
            rate = 1
            for symbol in shortest_chain:
                market_price = self.__get_market_price(symbol, order_type=order_type, fall_back_to_api=fall_back_to_api)
                rate = (rate / market_price)
        else:
            all_pairs_with: List[Instrument] = self.get_all_pairs_with(end)
            chains = []
            for instrument in all_pairs_with:
                for bichain in bidirectional_conversion_chain(self.graph, start, instrument):
                    chains.append(bichain)

            chains = shortest_conversion_chains(chains)

            if not len(chains):
                raise DataClassException(f"Error, could not find any chains for {start} to {end}!")
            if len(chains) == 1:
                chain = chains[0]
            else:
                if self.highest_liquidity_chain:
                    chain = self.__most_liquid_chain(chains)
                else:
                    chain = chains[0]

            rate = 1
            next_conversion: str = instrument.base_asset if instrument.base_asset != start else instrument.quote_asset

            for symbol in chain:
                instrument = filter_instruments(self.instruments, symbol)[0]
                market_price = self.__get_market_price(instrument, order_type=order_type,
                                                       fall_back_to_api=fall_back_to_api)
                if instrument.base_asset == next_conversion:
                    rate = (rate / market_price)
                    next_conversion = instrument.quote_asset
                else:
                    rate = market_price * rate
                    next_conversion = instrument.base_asset

        return rate * amount

    def __get_market_price(self,
                           symbol_or_instrument: Union[str, Instrument],
                           order_type: Optional[Market] = None,
                           fall_back_to_api: Optional[bool] = False
                           ) -> float:
        """
        Get the market price based on order_type

        :param symbol_or_instrument: The symbol or instrument
        :param order_type: The order type
        :param fall_back_to_api: To pass to quoter
        :return: The market price
        :raises DataClassException: If bad order type
        """
        if order_type and order_type != Market.BUY and order_type != Market.SELL:
            raise DataClassException(f"{order_type} is not a valid order type of BUY or SELL")
        if isinstance(symbol_or_instrument, str):
            symbol = symbol_or_instrument
        elif isinstance(symbol_or_instrument, Instrument):
            symbol = symbol_or_instrument.symbol
        else:
            raise DataClassException(f"Unknown type for symbol_or_instrument: {symbol_or_instrument}")

        if order_type:
            if order_type == Market.BUY:
                market_price = self.quoter.get_bid(symbol, fall_back_to_api=fall_back_to_api)
            else:
                market_price = self.quoter.get_ask(symbol, fall_back_to_api=fall_back_to_api)
        else:
            market_price = self.quoter.get_midpoint(symbol, fall_back_to_api=fall_back_to_api)
        return market_price
