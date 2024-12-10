import unittest
import sys
import logging
from symphony.client import BinanceClient
from symphony.data_classes import filter_instruments
from symphony.utils.graph import build_graph, find_path, find_all_paths, path_to_conversion_chain, \
    find_shortest_path, filter_conversion_chain, shortest_conversion_chains, \
    bidirectional_conversion_chain, verify_chain, get_execution_chain
from symphony.enum import Exchange, StableCoin, Market
from time import perf_counter
from symphony.config import USE_MODIN

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

binance_client = BinanceClient()
instruments = binance_client.get_all_instruments()

class BinanceGraphTest(unittest.TestCase):

    def test_build_graph(self):
        graph = build_graph(instruments)
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_find_path(self):
        graph = build_graph(instruments)
        path = find_path(graph, "EUR", "QLC")
        conversion_chain = path_to_conversion_chain(path)
        self.assertEquals(conversion_chain, ['BTCEUR', 'ETHBTC', 'QLCETH'])
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_find_all_paths(self):
        graph = build_graph(instruments)
        paths = find_all_paths(graph, "EUR", "QLC")
        self.assertEquals(paths, [['EUR', 'BTC', 'ETH', 'QLC'], ['EUR', 'BTC', 'QLC'], ['EUR', 'ETH', 'QLC']])
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_find_shortest_path(self):
        graph = build_graph(instruments)
        shortest_path = find_shortest_path(graph, "EUR", "QLC")
        self.assertEquals(shortest_path, ['EUR', 'BTC', 'QLC'])
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_conversion_chains(self):
        graph = build_graph(instruments)
        paths = find_all_paths(graph, "EUR", "QLC")
        conversion_chains = [path_to_conversion_chain(path) for path in paths]
        self.assertEquals(conversion_chains, [['BTCEUR', 'ETHBTC', 'QLCETH'], ['BTCEUR', 'QLCBTC'], ['ETHEUR', 'QLCETH']])
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_filter_conversion_chains(self):
        graph = build_graph(instruments)
        paths = find_all_paths(graph, "EUR", "QLC")
        conversion_chains = [path_to_conversion_chain(path) for path in paths]
        conversion_chains = filter_conversion_chain(conversion_chains, "QLCETH")
        self.assertEquals(conversion_chains, [['BTCEUR', 'ETHBTC', 'QLCETH'], ['ETHEUR', 'QLCETH']])
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_shortest_conversion_chains(self):
        graph = build_graph(instruments)
        paths = find_all_paths(graph, "EUR", "QLC")
        conversion_chains = [path_to_conversion_chain(path) for path in paths]
        conversion_chains = filter_conversion_chain(conversion_chains, "QLCETH")
        shortest_chain = shortest_conversion_chains(conversion_chains)[0]
        self.assertEquals(shortest_chain, ['ETHEUR', 'QLCETH'])
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_all_chains(self):
        graph = build_graph(instruments)
        fiat = "EUR"
        print_chains = True
        """
        for instrument in instruments:
            if instrument.contains_stablecoin() and \
                    instrument.stablecoin in Exchange.BINANCE.supported_stablecoins:
                conversion_chain = chain_with_stablecoin(graph, fiat, instrument)

                if print_chains:
                    print(f"With fiat [{fiat}] and target: {instrument.symbol}\n\t"
                          f"Found stablecoin chain: {conversion_chain}")
            else:
                paths = find_all_paths(graph, fiat, instrument.base_asset)
                conversion_chains = [path_to_conversion_chain(path) for path in paths]
                if instrument.symbol == 'BNBNGN':
                    breakpoint()
                    bidirectional_conversion_chain(graph, fiat, instrument)
                conversion_chains = filter_conversion_chain(conversion_chains, instrument.symbol)
                shortest_chain = shortest_conversion_chain(conversion_chains)

            if print_chains:
                print(f"With fiat [{fiat}] and target: {instrument.symbol}\n\t"
                      f"Found chains: {conversion_chains}\n\tShortest Chain: {shortest_chain}")
        """
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


    def test_bidirectional_chain_all_startingpoints(self):
        graph = build_graph(instruments)
        print_chains = False
        all_assets = []
        for instrument in instruments:
            all_assets.append(instrument.base_asset)
            all_assets.append(instrument.quote_asset)
        all_assets = list(set(all_assets))

        times = []
        for asset in all_assets:
            for instrument in instruments:
                start_time: float = perf_counter()
                conversion_chain = bidirectional_conversion_chain(graph, asset, instrument)
                end_time: float = perf_counter()
                times.append(end_time - start_time)

                if print_chains:
                    print(f"Found bidirectional chain: {conversion_chain}")

                self.assertGreaterEqual(len(conversion_chain), 1)
                for chain in conversion_chain:
                    # Make sure duplicates are never found
                    self.assertEquals(len(chain), len(list(set(chain))))
                    # Make sure chain not empty
                    self.assertNotEquals(chain, [])
                    for pair in chain:
                        # Make sure these are real pairs
                        self.assertEquals(len(filter_instruments(instruments, pair)), 1)
                    # Verify the chain
                    self.assertEquals(verify_chain(chain, asset, instruments), True)

        print("Average time to get chain: {:10.4f}s, Max time: {:10.4f}s".format(sum(times)/len(times), max(times)))
        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_execution_chain(self):
        graph = build_graph(instruments)
        start = "EUR"
        for instrument in instruments:
            chain = bidirectional_conversion_chain(graph, start, instrument)
            buy_exec_chain = get_execution_chain(chain, start, instruments, Market.BUY)
            sell_exec_chain = get_execution_chain(chain, start, instruments, Market.SELL)

            for i, order_type in enumerate(buy_exec_chain):
                if order_type == Market.BUY:
                    self.assertEquals(sell_exec_chain[i], Market.SELL)
                elif order_type == Market.SELL:
                    self.assertEquals(sell_exec_chain[i], Market.BUY)

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("BinanceGraphTest.test_build_graph").setLevel(logging.DEBUG)
    unittest.main()
