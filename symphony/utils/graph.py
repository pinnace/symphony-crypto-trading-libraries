from typing import List, Dict, NewType, Union
from symphony.data_classes import Instrument, filter_instruments
from symphony.exceptions import UtilityClassException
from symphony.enum import Market, StableCoin

CurrencyConversionGraph = NewType("CurrencyConversionGraph", Dict[str, List[str]])
ConversionChainType = NewType("ConversionChain", List[str])


# https://www.python.org/doc/essays/graphs/

def build_graph(instruments: List[Instrument]) -> CurrencyConversionGraph:
    """
    Utility for building a conversion graph from pair A -> pair B

    :param instruments: List of instruments
    :return: The conversion graph
    :rtype: Dict[str, List[str]]
    """
    if len(instruments) <= 1:
        raise UtilityClassException(f"List of instruments too small to create graph")

    if not isinstance(instruments[0], Instrument):
        raise UtilityClassException(f"Type not list of instruments: {type(instruments[0])}")

    graph: CurrencyConversionGraph = {}
    for instrument in instruments:
        if instrument.quote_asset not in graph.keys():
            graph[instrument.quote_asset] = []
        if instrument.base_asset not in graph[instrument.quote_asset]:
            graph[instrument.quote_asset].append(instrument.base_asset)
    return graph


def find_path(graph: CurrencyConversionGraph, start: str, end: str, path: List = []) -> List[str]:
    """
    Finds the first path from start to end

    :param graph: The conversion graph
    :param start: Start key (as quote)
    :param end: End key (as base)
    :param path: Used in recursion
    :return: The path
    """
    path = path + [start]
    if start == end:
        return path
    if start not in graph.keys():
        return None
    for node in graph[start]:
        if node not in path:
            newpath = find_path(graph, node, end, path)
            if newpath:
                return newpath
    return None


def find_all_paths(graph: CurrencyConversionGraph, start: str, end: str, path: List = []) -> List[str]:
    """
    Returns all possible paths to a target base currency

    :param graph: The conversion graph
    :param start: Start key (as quote)
    :param end: End key (as base)
    :param path: Used in recursion
    :return: The path
    """
    path = path + [start]
    if start == end:
        return [path]
    if start not in graph.keys():
        return []
    paths = []
    for node in graph[start]:
        if node not in path:
            newpaths = find_all_paths(graph, node, end, path)
            for newpath in newpaths:
                paths.append(newpath)
    return paths


def find_shortest_path(graph: CurrencyConversionGraph, start: str, end: str, path: List = []) -> List[str]:
    """
    Finds the shortest path to a given base currency from a given quote currency

    :param graph: The conversion graph
    :param start: Start key (as quote)
    :param end: End key (as base)
    :param path: Used in recursion
    :return: The path
    """
    path = path + [start]
    if start == end:
        return path
    if start not in graph.keys():
        return None
    shortest = None
    for node in graph[start]:
        if node not in path:
            newpath = find_shortest_path(graph, node, end, path)
            if newpath:
                if not shortest or len(newpath) < len(shortest):
                    shortest = newpath
    return shortest


def path_to_conversion_chain(path: List[str]) -> ConversionChainType:
    """
    Converts a path to conversion chain

    :param path: The path to convert
    :return: The conversion chain
    """
    conversion_chain: List[str] = []
    for i, s in enumerate(path[1:]):
        conversion_chain.append(s + path[i])
    return conversion_chain


def filter_conversion_chain(
        conversion_chains: List[ConversionChainType],
        target_instrument_or_symbol: Union[Instrument, str]
) -> List[ConversionChainType]:
    """
    Filters a set of conversion chains to target a specific pairing

    :param conversion_chains: List of conversion chains
    :param target_instrument_or_symbol: The target (i.e. last symbol in the chain).
    :return: List of filtered conversion chains
    :raises UtilityClassException: If the target type is unknown, if no chains were found (chains should be found in normal usage)
    """
    if isinstance(target_instrument_or_symbol, str):
        target_symbol = target_instrument_or_symbol
    elif isinstance(target_instrument_or_symbol, Instrument):
        target_symbol = target_instrument_or_symbol.symbol
    else:
        raise UtilityClassException(f"Unknown type: {type(target_instrument_or_symbol)}")

    filtered_conversion_chains = [chain for chain in conversion_chains if chain[-1] == target_symbol]
    if not len(filtered_conversion_chains):
        raise UtilityClassException(f"Could not filter the conversion chains with symbol: "
                                    f"{target_instrument_or_symbol}, chains: {conversion_chains}")
    else:
        return filtered_conversion_chains


def shortest_conversion_chains(conversion_chains: List[ConversionChainType]) -> List[ConversionChainType]:
    """
    Identifies the shortest conversion chains. Returns shortest chains.

    :param conversion_chains: List of conversion chains
    :return: Shortest chains
    """
    min_length: int = 0
    for i, chain in enumerate(conversion_chains):
        if not min_length or len(chain) < min_length:
            min_length = len(chain)
    return [chain for chain in conversion_chains if len(chain) == min_length]


def bidirectional_conversion_chain(graph: CurrencyConversionGraph, start: str,
                                   end_instrument: Instrument) -> List[ConversionChainType]:
    """
    Gets a (potentially) birectional chain, i.e. a chain which may involve different types of
    market orders to enter positions. This function will favor 'forward-only' chains (i.e.
    conversions that involve a successive series of buys OR successive sells), but will fall back to
    a bidirectional (i.e. mixed buys and sells) chain if no forward chains were found. Max chain
    size is 4 symbols. Recursive.

    :param graph: The conversion graph
    :param start: Starting asset (e.g. 'EUR', 'BTC', 'ADA')
    :param end_instrument: Target instrument
    :return: The identified set of conversion chains
    :raises UtilityClassException: If a chain has not been found
    """
    if start == end_instrument.base_asset or start == end_instrument.quote_asset:
        return [[end_instrument.symbol]]

    if paths := find_all_paths(graph, start, end_instrument.base_asset):
        conversion_chains = [path_to_conversion_chain(path) for path in paths]
        try:
            return filter_conversion_chain(conversion_chains, end_instrument.symbol)
        except UtilityClassException:
            pass
        except Exception as e:
            raise e

    chains = []
    # Basic circumstance. Start is a base currency instead of counter
    if start not in graph.keys():
        non_quote_pairs = []
        for key in graph.keys():
            if start in graph[key]:
                non_quote_pairs.append((start, key))
        for start_curr, new_start in non_quote_pairs:
            new_chains = bidirectional_conversion_chain(graph, new_start, end_instrument)
            for new_chain in new_chains:
                if start_curr + new_start == new_chain[0]:
                    continue
                chains.append(
                    [start_curr + new_start] + new_chain
                )

    elif end_instrument.quote_asset not in graph[start]:
        if start in graph[end_instrument.quote_asset]:
            return [[start + end_instrument.quote_asset, end_instrument.symbol]]
        all_pairs_as_quote = [(base, start) for base in graph[start]]

        for base, start_curr in all_pairs_as_quote:

            if base in graph.keys() and end_instrument.base_asset in graph[base]:
                chains.append(
                    [base + start_curr, end_instrument.base_asset + base, end_instrument.symbol]
                )
            elif end_instrument.quote_asset in graph.keys() and base in graph[end_instrument.quote_asset]:
                intermediate_pair = base + end_instrument.quote_asset
                if intermediate_pair == end_instrument.symbol:
                    chains.append(
                        [base + start_curr, end_instrument.symbol]
                    )
                else:
                    chains.append(
                        [base + start_curr, base + end_instrument.quote_asset, end_instrument.symbol]
                    )
    # The really difficult ones
    if not chains:
        quote_combinations = []
        base_combinations = []
        if end_instrument.quote_asset in graph.keys():
            quote_combinations = [(base, end_instrument.quote_asset) for base in graph[end_instrument.quote_asset]]
        for key in graph.keys():
            if end_instrument.quote_asset in graph[key]:
                base_combinations.append((end_instrument.quote_asset, key))
        total_len = len(quote_combinations) + len(base_combinations)
        all_combinations = quote_combinations + base_combinations
        if total_len == 1:
            base, quote = all_combinations[0]
            if base in graph.keys() and start in graph[base]:
                chains.append([start + base, end_instrument.symbol])
        elif total_len >= 1:
            for combo in all_combinations:
                combo_base, combo_quote = combo
                if end_instrument.base_currency == combo_base:
                    if combo_base in graph.keys() and start in graph[combo_base]:
                        chains.append([start + combo_base, end_instrument.symbol])
        # Depth 2
        if not chains:
            start_quote_combinations = []
            start_base_combinations = []
            if start in graph.keys():
                start_quote_combinations = [(base, start) for base in graph[start]]
            for key in graph.keys():
                if start in graph[key]:
                    start_base_combinations.append((start, key))
            all_start_combinations = start_quote_combinations + start_base_combinations
            for combo in all_combinations:
                base, quote = combo
                if base + quote == end_instrument.symbol:
                    continue
                for start_combination in all_start_combinations:
                    b, q = start_combination
                    conversion_instrument = b if b != start else q
                    if conversion_instrument == base or conversion_instrument == quote:
                        chains.append(
                            [b + q, base + quote, end_instrument.symbol]
                        )

    if not chains:
        raise UtilityClassException(
            f"Could not derive bidirectional chain! Start: {start}, Symbol: {end_instrument.symbol}")
    return chains


def get_instrument_chain(chain: ConversionChainType, instruments: List[Instrument]) -> List[Instrument]:
    """
    Converts a conversion chain to a list of instruments

    :param chain: The conversion chain
    :param instruments: Exchange instruments
    :return: List of Instruments
    """
    instrument_chain: List[Instrument] = []
    symbol: str
    for symbol in chain:
        instrument = filter_instruments(instruments, symbol)[0]
        instrument_chain.append(instrument)
    return instrument_chain


def verify_chain(chain: ConversionChainType, start: str, instruments: List[Instrument]) -> bool:
    """
    Verifies that the conversion chain is executable (i.e. all instruments exist and at least one
    part of the pair is present in the next)

    :param chain: The conversion chain
    :param start: The starting asset (e.g. 'EUR', 'BTC')
    :param instruments: List of exchange instruments
    :return: True or False
    """

    next_conversion: str = start
    instruments: List[Instrument] = get_instrument_chain(chain, instruments)
    for instrument in instruments:
        if instrument.base_asset == next_conversion:
            next_conversion = instrument.quote_asset
        elif instrument.quote_asset == next_conversion:
            next_conversion = instrument.base_asset
        else:
            return False
    return True


def get_execution_chain(chain: ConversionChainType,
                        start: str,
                        instruments: List[Instrument],
                        ) -> List[Market]:
    """
    Returns the execution chain

    :param chain: Conversion chain
    :param start: Starting asset
    :param instruments: Exchange Instruments
    :return: List of market order types, either BUY or SELL
    """

    executionchain: List[Market] = []
    instrument_chain: List[Instrument] = get_instrument_chain(chain, instruments)

    next_conversion: str = start
    for instrument in instrument_chain:
        if instrument.quote_asset == next_conversion:
            next_conversion = instrument.base_asset
            executionchain.append(Market.BUY)
        elif instrument.base_asset == next_conversion:
            next_conversion = instrument.quote_asset
            executionchain.append(Market.SELL)
    return executionchain




