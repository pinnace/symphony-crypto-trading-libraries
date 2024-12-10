from symphony.client import BinanceClient
import pandas as pd
from symphony.data_classes import PriceHistory, Instrument
from symphony.enum import Timeframe, string_to_timeframe
import json


def handler(event, context):
    try:
        timeframe = string_to_timeframe(event["timeframe"])
    except Exception as e:
        return {
            "e": f"Failed to parse timeframe: {str(e)}"
        }

    if "symbol" not in event.keys():
        return {
            "e": "Symbol not defined"
        }

    if "/" not in event["symbol"]:
        return {
            "e": "Symbol must have '/' inbetween base and quote asset"
        }
    else:
        base_asset = event["symbol"].split("/")[0]
        quote_asset = event["symbol"].split("/")[1]
        instrument = Instrument(symbol=base_asset + quote_asset, base_asset=base_asset, quote_asset=quote_asset)

    if "num_bars" not in event.keys():
        num_bars = 400
    else:
        num_bars = int(event["num_bars"])

    ph = BinanceClient.anon_get(instrument, timeframe, num_bars_or_start_time=num_bars)
    result = ph.price_history.to_json()
    parsed = json.loads(result)
    return parsed