from symphony.client import BinanceClient
import os
import signal
import sys
import pickle
import random
from symphony.backtest.results.results_helper import ResultsHelper

results_helper = ResultsHelper('demark')



def signal_handler(sig, frame):
    print('Exiting....')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

jesse_cmd = "jesse import-candles 'Binance' '{}' '2019-06-01' --skip-confirmation"

instruments = [instrument for instrument in results_helper.instruments if instrument.isolated_margin_allowed and instrument.quote_asset in ["USDT", "USDC", "BUSD", "EUR", "PAX"]]
random.shuffle(instruments)
symbols_already_backtested = results_helper.results_df["Symbol"].unique()
#bc = BinanceClient()

for instrument in instruments:
    #if instrument.symbol in symbols_already_backtested:
    #    continue
    print(f"-------- Fetching candles for {instrument}")
    cmd = jesse_cmd.format(instrument.base_asset + "-" + instrument.quote_asset)
    os.system(cmd)
    print(f"-------- Done fetching {instrument}")

print("-------- Complete")

breakpoint()
