from symphony.client import BinanceClient
from symphony.ml import DemarkBuySetupClassifier
from symphony.enum import Timeframe, timeframe_to_numpy_string
from symphony.config import AWS_REGION
from symphony.data_classes import PriceHistory, Instrument
from symphony.indicator_v2.demark import bearish_price_flip, bullish_price_flip, td_buy_setup
from symphony.indicator_v2 import IndicatorRegistry
from symphony.risk_management import CryptoPositionSizer
from symphony.quoter import BinanceRealTimeQuoter
import json
import boto3
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

notification_template = \
"""
*Strategy*: DemarkBuySetup
*Symbol*: {0}
*Timeframe*: {1}
*Confidence*: {2}
*Timestamp*: {3}
*StopLoss*: {4}
*TakeProfit*: {5}
*Margin Deposit*: {6}
*Total Position Size*: {7}
"""

url = 'https://bdbzcqwwlg.execute-api.eu-west-1.amazonaws.com/production/prod/candles'
headers = {"Content-Type": "application/json"}

timeframe = Timeframe.H1
num_bars = 400

def apply_indicators(ph: PriceHistory) -> PriceHistory:
    bearish_price_flip(ph)
    bullish_price_flip(ph)
    td_buy_setup(ph)
    return ph

def get_candles(instrument: Instrument, timeframe: Timeframe, client):
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
    apply_indicators(ph)
    return ph



if __name__ == "__main__":
    bc = BinanceClient()

    #quoter = BinanceRealTimeQuoter(bc)
    #position_sizer = CryptoPositionSizer(quoter)
    buy_setup_classifer = DemarkBuySetupClassifier(use_s3=False)
    buy_setup_classifer.load_models(version_folder="latest")

    allowed_instruments = [instrument for instrument in bc.instruments if instrument.symbol in buy_setup_classifer.symbols and instrument.isolated_margin_allowed]

    client = boto3.client('lambda', region_name=AWS_REGION)
    futures = []
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        for instrument in allowed_instruments:
            futures.append(executor.submit(get_candles, instrument, timeframe, client))

        results = [future.result() for future in futures]

    breakpoint()

    breakpoint()
    """
    for instrument in allowed_instruments:
        print(f"Predicting: {instrument}")
        ph = BinanceClient.anon_get(instrument, timeframe, num_bars_or_start_time=num_bars)
        apply_indicators(ph)
        if ph.price_history[IndicatorRegistry.BUY_SETUP.value].iloc[-1] == 1:

            result = buy_setup_classifer.predict(ph)
            print(f"Setup found for {instrument.symbol} {timeframe}, Prediction: {result}")
    """