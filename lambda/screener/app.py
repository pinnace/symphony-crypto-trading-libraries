from symphony.client import BinanceClient
from symphony.notification import SlackNotifier
from symphony.ml import DemarkClassifier
from symphony.enum import Timeframe, timeframe_to_numpy_string
from symphony.config import AWS_REGION, AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID
from symphony.data_classes import PriceHistory, Instrument
from symphony.indicator_v2.demark import bearish_price_flip, bullish_price_flip, td_buy_setup, td_sell_setup, td_buy_countdown
from symphony.indicator_v2 import IndicatorRegistry
from symphony.risk_management import CryptoPositionSizer
from symphony.quoter import BinanceRealTimeQuoter
from symphony.enum.timeframe import string_to_timeframe
import json
import boto3
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import os
from time import sleep
import logging

num_bars = 500
threshold = 0.58
notification_template = \
"""
*Strategy*: {0}
*Symbol*: {1}
*Timeframe*: {2}
*Confidence*: {3}
*Timestamp*: {4}
"""
allowed_strategies = ["DemarkBuySetup", "DemarkBuyCountdown"]

def apply_indicators(ph: PriceHistory, strategy: str) -> PriceHistory:
    bearish_price_flip(ph)
    bullish_price_flip(ph)
    td_buy_setup(ph)
    if strategy == "DemarkBuyCountdown":
        td_sell_setup(ph)
        td_buy_countdown(ph, log_level=logging.ERROR)
    return ph


def get_candles(instrument: Instrument, timeframe: Timeframe, client, strategy: str):
    payload = {
        "symbol": instrument.base_asset + "/" + instrument.quote_asset,
        "timeframe": timeframe_to_numpy_string(timeframe),
        "num_bars": num_bars
    }
    response = client.invoke(
        FunctionName="candles",
        InvocationType="RequestResponse",
        Payload=bytes(json.dumps(payload), "utf8")
    )
    resp = response['Payload'].read()
    df = pd.read_json(resp)
    ph = PriceHistory(instrument=instrument, timeframe=timeframe, price_history=df)
    apply_indicators(ph, strategy)
    return ph

def get_strategy_indicator(strategy: str) -> str:
    if strategy == "DemarkBuySetup":
        return IndicatorRegistry.BUY_SETUP.value
    elif strategy == "DemarkBuyCountdown":
        return IndicatorRegistry.BUY_COUNTDOWN.value
    else:
        raise Exception(f"Unknown strategy {strategy}")

def handler(event, context):
    sleep(30)
    strategy = event["strategy"]
    if strategy not in allowed_strategies:
        raise Exception(f"Unknown strategy {strategy}")
    strategy_indicator = get_strategy_indicator(strategy)
    version = event["version"]

    bc = BinanceClient()
    notifier = SlackNotifier()
    timeframe_str = event["timeframe"]
    timeframe = string_to_timeframe(timeframe_str)
    #version = "latest-with-trend"
    #strategy = "DemarkBuySetup"
    try:
        classifier = DemarkClassifier(use_s3=False)
        classifier.load_models(strategy, version_folder=version)
    except Exception as e:
        print(str(e))
        exit()
    print(f"Models loaded. Strategy: {strategy}:{version}")

    allowed_instruments = [instrument for instrument in bc.instruments if
                           instrument.symbol in classifier.symbols and instrument.isolated_margin_allowed]

    print(f"Allowed Instruments: {allowed_instruments}")
    client = boto3.client('lambda', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
    futures = []
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        for instrument in allowed_instruments:
            futures.append(executor.submit(get_candles, instrument, timeframe, client, strategy))

        results = [future.result() for future in futures]
    ts = 0
    for ph in results:
        df = ph.price_history
        if not ts and ph.timeframe == timeframe:
            ts = str(df.index[-1])
        if ph.price_history[strategy_indicator].iloc[-1] == 1:
            pred = classifier.predict(ph, verbose=True)
            print(f"Event found for {ph.instrument} | {ph.timeframe} | {strategy}, with prediction: {pred} at time {str(df.index[-1])}")
            if pred >= threshold:
                print(f"Signal generated!")
                notification = notification_template.format(strategy, ph.instrument.symbol, timeframe_to_numpy_string(timeframe), pred, str(df.index[-1]))
                notifier.notify_message(notification, channel="signal")

    notifier.notify_message(f"Finished Screener run at: {str(ts)} for {timeframe} | {strategy}:{version}", channel="log")
    return {}

if __name__ == "__main__":
    handler({"timeframe": "1h", "strategy" : "DemarkBuyCountdown", "version": "latest"}, {})