import numpy as np

from symphony.config import config, BACKTEST_DIR, USE_MODIN, USE_S3
from symphony.enum.timeframe import Timeframe, timeframe_to_string, string_to_timeframe, integer_to_timeframe
from symphony.enum import Column
from symphony.data_classes import Instrument, PriceHistory
from symphony.backtest.results.results_helper import ResultsHelper
from symphony.data.archivers import BinanceArchiver
from symphony.indicator_v2.demark import td_buy_setup, td_sell_setup, bullish_price_flip, bearish_price_flip, td_differential, td_anti_differential, td_reverse_differential, td_trap, td_open, td_clop, td_camouflage, td_clopwin, td_buy_countdown
from symphony.indicator_v2.volatility import bollinger_bands
from symphony.indicator_v2.trend import sma
from symphony.indicator_v2.oscillators import zig_zag, get_closest_harmonic
from symphony.indicator_v2 import IndicatorRegistry
from symphony.data.archivers import BinanceArchiver

from concurrent.futures._base import ALL_COMPLETED
import concurrent.futures
from multiprocessing import cpu_count

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

strategy = "DemarkBuyCountdown"
if strategy == "DemarkBuySetup":
    strategy_core_indicator = IndicatorRegistry.BUY_SETUP.value
elif strategy == "DemarkSellSetup":
    strategy_core_indicator = IndicatorRegistry.SELL_SETUP.value

elif strategy == "DemarkBuyCountdown":
    strategy_core_indicator = IndicatorRegistry.BUY_COUNTDOWN.value
else:
    raise Exception("Unknown strategy")


histories = {}
mismatch_setup_errs = {}
no_timestamp_errs = {}
archiver = BinanceArchiver(config["archive"]["s3_bucket"], use_s3=USE_S3)
rh = ResultsHelper(strategy, use_s3=True)
rdf = rh.results_df
new_df = rdf.copy(deep=True)
timeframes = [Timeframe.H1, Timeframe.H4]

abs_start_ts = pd.Timestamp("2019-12-15 00:00:00", tz='utc')

new_indicators = [
    IndicatorRegistry.TD_DIFFERENTIAL.value,
    IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value,
    IndicatorRegistry.TD_CLOP.value,
    IndicatorRegistry.TD_CLOPWIN.value,
    IndicatorRegistry.TD_OPEN.value,
    IndicatorRegistry.TD_TRAP.value,
    IndicatorRegistry.TD_CAMOUFLAGE.value,
    IndicatorRegistry.BOLLINGER_BANDS_WIDTH.value,
    IndicatorRegistry.BOLLINGER_BANDS_PERCENT.value

]
for new_indi in new_indicators:
    if new_indi not in new_df.columns:
        new_df[new_indi] = np.zeros(len(new_df))

def apply_indicators(price_history: PriceHistory, indicator=None) -> PriceHistory:
    print(f"Applying indicators to {price_history.instrument.symbol} {price_history.timeframe}")
    if indicator != IndicatorRegistry.ZIGZAG.value:

        price_history = bullish_price_flip(price_history)
        price_history = bearish_price_flip(price_history)
        if strategy == "DemarkBuySetup":
            price_history = td_buy_setup(price_history)
        elif strategy == "DemarkSellSetup":
            price_history = td_sell_setup(price_history)
        elif strategy == "DemarkBuyCountdown":
            price_history = td_buy_setup(price_history)
            price_history = td_sell_setup(price_history)
            price_history = td_buy_countdown(price_history)
        else:
            raise Exception("Unknown strategy")


        price_history = td_clop(price_history)
        price_history = td_differential(price_history)
        price_history = td_anti_differential(price_history)
        price_history = td_clopwin(price_history)
        price_history = td_trap(price_history)
        price_history = td_open(price_history)
        price_history = td_camouflage(price_history)
        price_history = bollinger_bands(price_history)
        price_history = bollinger_bands(price_history)

        sma(price_history, period=50)
        sma(price_history, period=200)
    else:

        price_history = zig_zag(price_history)

    return price_history


def fetch_histories(symbols):
    def fetch_symbol(symbol = "", timeframe = None):
        symbol_phistory = archiver.read(symbol, timeframe)
        symbol_phistory.price_history = symbol_phistory.price_history.loc[abs_start_ts:]
        histories[symbol][timeframe] = symbol_phistory
        print(f"Fetching {symbol} {timeframe}")
        return

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:

        for symbol in symbols:
            for timeframe in timeframes:
                if symbol not in histories.keys():
                    histories[symbol] = {}
                if timeframe not in histories[symbol].keys():
                    histories[symbol][timeframe] = None
                futures.append(executor.submit(fetch_symbol, symbol=symbol, timeframe=timeframe))

        concurrent.futures.wait(futures, timeout=None, return_when=ALL_COMPLETED)

    futures = []
    with concurrent.futures.ProcessPoolExecutor(cpu_count() - 1) as executor:
        for symbol in histories.keys():
            for timeframe in histories[symbol].keys():
                futures.append(executor.submit(apply_indicators, histories[symbol][timeframe]))
        concurrent.futures.wait(futures, timeout=None, return_when=ALL_COMPLETED)
        results = [future.result() for future in futures]

    for result in results:
        result: PriceHistory
        histories[result.instrument.symbol][result.timeframe] = result

    return

def populate(row, indi=None, freeze_errs=False):
    symbol = row["Symbol"]
    timeframe = integer_to_timeframe(int(row["Timeframe"]))
    ts = pd.Timestamp(row["EntryTimestamp"], tz='utc')
    ph: pd.DataFrame = histories[symbol][timeframe].price_history

    try:
        if indi != IndicatorRegistry.ZIGZAG.value and indi != "BollingerOutsideClose" and indi != "Trend":
            val = ph.loc[ts][indi]
        elif indi == "BollingerOutsideClose":
            val = False
            if strategy == "DemarkBuySetup" and ph.loc[ts][Column.CLOSE] < ph.loc[ts][IndicatorRegistry.BOLLINGER_BANDS_LOWER.value]:
                val = True
            elif strategy == "DemarkSellSetup" and ph.loc[ts][Column.CLOSE] > ph.loc[ts][IndicatorRegistry.BOLLINGER_BANDS_UPPER.value]:
                val = True
            elif strategy == "DemarkBuyCountdown" and ph.loc[ts][Column.CLOSE] < ph.loc[ts][IndicatorRegistry.BOLLINGER_BANDS_LOWER.value]:
                val = True
        elif indi == "Trend":
            if ph.loc[ts][IndicatorRegistry.SMA_50.value] > ph.loc[ts][IndicatorRegistry.SMA_200.value]:
                val = "UP"
            else:
                val = "DOWN"
        else:
            df = ph.copy(deep=True).loc[:ts].iloc[-800:]
            pit_ph = PriceHistory(price_history=df, instrument=Instrument(symbol=symbol))
            apply_indicators(pit_ph, indicator=indi)
            val = get_closest_harmonic(pit_ph, index=ts, verbose=True)
        missing_key = False
    except Exception as e:
        err = str(e)
        if not freeze_errs:
            if symbol not in no_timestamp_errs.keys():
                no_timestamp_errs[symbol] = 0

            print(f"Missing timestamp for {symbol} at {ts}")
            no_timestamp_errs[symbol] += 1
        missing_key = True
        val = 0

    if not missing_key and ph.loc[ts][strategy_core_indicator] != 1:
        if not freeze_errs:
            if symbol not in mismatch_setup_errs.keys():
                mismatch_setup_errs[symbol] = 0
            print(f"Pattern not found for {symbol} at {ts}")
            mismatch_setup_errs[symbol] += 1

    return val

if __name__ == "__main__":

    symbols = rdf["Symbol"].unique()
    fetch_histories(symbols)


    print("Applying Harmonic")
    new_df[IndicatorRegistry.HARMONIC.value] = new_df.apply(populate, axis=1, indi=IndicatorRegistry.ZIGZAG.value)
    print("Applying TD DIff")
    new_df[IndicatorRegistry.TD_DIFFERENTIAL.value] = new_df.apply(populate, axis=1, indi=IndicatorRegistry.TD_DIFFERENTIAL.value)
    print("Applying TD Anti DIff")
    new_df[IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value] = new_df.apply(populate, axis=1, indi=IndicatorRegistry.TD_ANTI_DIFFERENTIAL.value, freeze_errs=True)

    print("Applying CLOP")
    new_df[IndicatorRegistry.TD_CLOP.value] = new_df.apply(populate, axis=1, indi=IndicatorRegistry.TD_CLOP.value, freeze_errs=True)

    print("Applying CLOPWIN")
    new_df[IndicatorRegistry.TD_CLOPWIN.value] = new_df.apply(populate, axis=1, indi=IndicatorRegistry.TD_CLOPWIN.value, freeze_errs=True)
    print("Applying OPEN")
    new_df[IndicatorRegistry.TD_OPEN.value] = new_df.apply(populate, axis=1, indi=IndicatorRegistry.TD_OPEN.value, freeze_errs=True)
    print("Applying TRAP")
    new_df[IndicatorRegistry.TD_TRAP.value] = new_df.apply(populate, axis=1, indi=IndicatorRegistry.TD_TRAP.value, freeze_errs=True)
    print("Applying CAMOUFLAGE")
    new_df[IndicatorRegistry.TD_CAMOUFLAGE.value] = new_df.apply(populate, axis=1, indi=IndicatorRegistry.TD_CAMOUFLAGE.value, freeze_errs=True)
    print("Applying BB %")
    new_df[IndicatorRegistry.BOLLINGER_BANDS_PERCENT.value] = new_df.apply(populate, axis=1, indi=IndicatorRegistry.BOLLINGER_BANDS_PERCENT.value, freeze_errs=True)
    print("Applying BB Width")
    new_df[IndicatorRegistry.BOLLINGER_BANDS_WIDTH.value] = new_df.apply(populate, axis=1, indi=IndicatorRegistry.BOLLINGER_BANDS_WIDTH.value, freeze_errs=True)

    print("Applying BollingerOutsideClose")
    new_df["BollingerOutsideClose"] = new_df.apply(populate, axis=1, indi="BollingerOutsideClose", freeze_errs=True)

    new_df["Trend"] = new_df.apply(populate, axis=1, indi="Trend", freeze_errs=True)
    breakpoint()
    """
    for symbol in rdf["Symbol"].unique():
        symbol_data = archiver.read(symbol, Timeframe.H1)
        symbol_data = apply_indicators(symbol_data)
        srdf = rdf[rdf["Symbol"] == symbol]
        for row in srdf.iterrows():
            breakpoint()
    """