from symphony.data_classes import PriceHistory, Instrument
from symphony.enum import Timeframe
from typing import List
from symphony.config import BACKTEST_DIR
import pandas as pd
import json
import pathlib
import pickle


def dummy_td_countdown_data() -> PriceHistory:
    """
    Creates dummy testing data for TD Demark test

    :return: (`PriceHistory`)
    """
    loc = pathlib.Path(__file__)
    with open(str(loc.parent) + "/data/test-price-history-raw-merged.json") as data_file:
        data = json.load(data_file)

    instrument = Instrument(symbol=data["instrument"])
    df = pd.DataFrame({
        "timestamp": [],
        "open": [],
        "high": [],
        "low": [],
        "close": [],
        "volume": [],
        "TEST_BULLISH_PRICE_FLIP": [],
        "TEST_BEARISH_PRICE_FLIP": [],
        "TEST_SELL_SETUP": [],
        "TEST_BUY_SETUP": [],
        "TEST_TDST_RESISTANCE": [],
        "TEST_TDST_SUPPORT": [],
        "TEST_SELL_COUNTDOWN": [],
        "TEST_BUY_COUNTDOWN": []

    })
    for candle in data["price_history"]:
        row = {
            "timestamp": candle["v"][0],
            "open": candle["v"][1],
            "high": candle["v"][2],
            "low": candle["v"][3],
            "close": candle["v"][4],
            "volume": candle["v"][5],
            "TEST_BULLISH_PRICE_FLIP": 1 if candle["cd"]["setup_index"] == 1 and candle["cd"][
                "setup_type"] == "SELL" else 0,
            "TEST_BEARISH_PRICE_FLIP": 1 if candle["cd"]["setup_index"] == 1 and candle["cd"][
                "setup_type"] == "BUY" else 0,
            "TEST_SELL_SETUP": candle["cd"]["setup_index"] if candle["cd"]["setup_type"] == "SELL" else 0,
            "TEST_BUY_SETUP": candle["cd"]["setup_index"] if candle["cd"]["setup_type"] == "BUY" else 0,
            "TEST_TDST_RESISTANCE": candle["cd"]["tdst_resistance"],
            "TEST_TDST_SUPPORT": candle["cd"]["tdst_support"],
            "TEST_SELL_COUNTDOWN": candle["cd"]["countdown_index"] if candle["cd"]["countdown_type"] == "SELL" else 0,
            "TEST_BUY_COUNTDOWN": candle["cd"]["countdown_index"]  if candle["cd"]["countdown_type"] == "BUY" else 0,
            "TEST_BUY_COMBO": 1 if candle["cd"]["is_combo_bar_13"] and candle["cd"]["combo_type"] == "BUY" else 0,
            "TEST_SELL_COMBO": 1 if candle["cd"]["is_combo_bar_13"] and candle["cd"]["combo_type"] == "SELL" else 0,
            "TEST_AGGRESSIVE_BUY_COUNTDOWN": 1 if candle["cd"]["is_aggressive_bar_13"] and candle["cd"]["aggressive_type"] == "SELL" else 0,
            "TEST_AGGRESSIVE_SELL_COUNTDOWN": 1 if candle["cd"]["is_aggressive_bar_13"] and candle["cd"]["aggressive_type"] == "BUY" else 0
        }
        df = df.append(row, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s')
    df = df.set_index('timestamp')
    return PriceHistory(instrument=instrument, price_history=df, timeframe=Timeframe.H1)

def dummy_instruments() -> List[Instrument]:
    instruments_file = pathlib.Path(BACKTEST_DIR + "test_data/" + "instruments.pkl")
    fh = instruments_file.open("rb+")
    instruments = pickle.load(fh)
    return instruments
