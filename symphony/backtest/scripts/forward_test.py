from symphony.client import BinanceClient
from symphony.notification import SlackNotifier
from symphony.ml import DemarkBuySetupClassifier
from symphony.enum import Timeframe, timeframe_to_numpy_string
from symphony.utils.time import get_timestamp_of_num_bars_back, get_num_bars_timestamp_to_present
from symphony.config import AWS_REGION, AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID
from symphony.data_classes import PriceHistory, Instrument
from symphony.indicator_v2.demark import bearish_price_flip, bullish_price_flip, td_buy_setup, td_sell_setup
from symphony.indicator_v2 import IndicatorRegistry
from symphony.risk_management import CryptoPositionSizer
from symphony.quoter import BinanceRealTimeQuoter
import json
import boto3
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import os
from typing import List

strategy = "DemarkBuySetup"
if strategy == "DemarkBuySetup":
    core_indicator = IndicatorRegistry.BUY_SETUP.value
elif strategy == "DemarkSellSetup":
    core_indicator = IndicatorRegistry.SELL_SETUP.value
else:
    raise Exception("Unknown strategy")
version = "all-except-natr"

timeframes = [Timeframe.H1, Timeframe.H4]
threshold = 0.55
start_timestamp = pd.Timestamp("2021-05-01 00:00:00", tz='utc')
num_bars = get_num_bars_timestamp_to_present(start_timestamp, Timeframe.H1)

def apply_indicators(ph: PriceHistory) -> PriceHistory:
    bearish_price_flip(ph)
    bullish_price_flip(ph)
    td_buy_setup(ph)
    return ph

def get_candles(instrument: Instrument, timeframe: Timeframe) -> PriceHistory:
    return BinanceClient.anon_get(instrument, timeframe, start_timestamp)

def apply_indicators(ph: PriceHistory) -> PriceHistory:
    bearish_price_flip(ph)
    bullish_price_flip(ph)
    td_buy_setup(ph)
    td_sell_setup(ph)
    return ph

def get_price_histories(allowed_instruments: List[Instrument]) -> List[PriceHistory]:
    futures = []
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        for instrument in allowed_instruments:
            for timeframe in timeframes:
                futures.append(executor.submit(get_candles, instrument, timeframe))

        results = [future.result() for future in futures]

    return results

if __name__ == "__main__":

    bc = BinanceClient()

    try:
        if strategy == "DemarkBuySetup":
            setup_classifer = DemarkBuySetupClassifier(use_s3=False)
            setup_classifer.load_models(version_folder=version)
        else:
            raise Exception("Unimplemented")
    except Exception as e:
        print(str(e))
        exit()
        
    print(f"Models loaded. Strategy: {strategy}:{version}")

    allowed_instruments = [instrument for instrument in bc.instruments if
                           instrument.symbol in setup_classifer.symbols and instrument.isolated_margin_allowed]
    histories = get_price_histories(allowed_instruments)
    print("Data fetched")

    for history in histories:
        apply_indicators(history)
    print("Indicators applied")

    date_counts = {}
    preds = []
    for history in histories:
        df = history.price_history
        for setup_index in df[df[core_indicator] == 1].index:
            if str(setup_index) not in date_counts.keys():
                date_counts[str(setup_index)] = 0
            date_counts[str(setup_index)] += 1
            pred = setup_classifer.predict(history, index=setup_index)
            preds.append(pred)
            print(f"{strategy} prediction for {history.instrument.symbol} | {history.timeframe} @ {str(setup_index)}: {pred}: {'BUY' if pred > threshold else ''}")
    breakpoint()
