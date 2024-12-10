import pandas as pd
import psycopg2
from symphony.config import config, BACKTEST_DIR
from symphony.enum.timeframe import Timeframe, timeframe_to_string
from symphony.data_classes.instrument import Instrument
from symphony.backtest.results.results_helper import ResultsHelper
import json
import os
import random

strategy = 'DemarkBuySetup'
suffix = '-Pressure-Demarker'
timeframes = [Timeframe.H4]
con = psycopg2.connect(database=config["jesse"]["jesse_db"], user=config["jesse"]["jesse_user"],
                       password=config["jesse"]["jesse_password"], host=config["jesse"]["jesse_host"], port="5432")
results_helper = ResultsHelper(strategy + suffix, use_s3=False)
absolute_earliest_date = pd.Timestamp('2020-01-01')
to_date = pd.Timestamp('2021-05-28')


def get_symbols():
    cur = con.cursor()
    cur.execute("select distinct symbol from candle;")
    rows = cur.fetchall()

    # Contraints; each route must have same timeframe and all symbols with same quote asset
    symbols_dict = {}
    symbols = list(list(zip(*rows))[0])

    for symbol in symbols:
        quote_asset = symbol.split("-")[1]
        if quote_asset not in symbols_dict.keys():
            symbols_dict[quote_asset] = []
        symbols_dict[quote_asset].append(symbol)
    return symbols_dict


def get_earliest_date(symbol: str) -> pd.Timestamp:
    cur = con.cursor()
    cur.execute(f"select min(timestamp) from candle where symbol = '{symbol}';")
    rows = cur.fetchall()
    unix_time = rows[0][0]
    timestamp = pd.Timestamp(unix_time, unit='ms') + pd.Timedelta("9 days")
    return timestamp


def tf_to_str(timeframe: Timeframe) -> str:
    if timeframe == Timeframe.H1:
        return "1h"
    elif timeframe == Timeframe.H4:
        return "4h"
    return ""


def build_routes(symbols_dict):
    list_of_routes = []
    symbols = []
    for quote_asset in symbols_dict.keys():
        for timeframe in timeframes:
            route = []
            for symbol in symbols_dict[quote_asset]:
                if quote_asset in ["USDT", "USDC", "BUSD", "EUR"]:
                    list_of_routes.append(
                        [('Binance', symbol, tf_to_str(timeframe), strategy)]
                    )
                    symbols.append(symbol)
    return symbols, list_of_routes


def write_template_file(routes):
    template_file = """
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Make sure to read the docs about routes if you haven't already:
# https://docs.jesse.trade/docs/routes.html
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from jesse.utils import anchor_timeframe

# trading routes
routes = {0}

# in case your strategy requires extra candles, timeframes, ...
extra_candles = [
]


""".format(str(routes).replace("[(", "[\n\t(").replace("), ", "), \n"))

    template_file_name = "routes.py"

    with open(BACKTEST_DIR + template_file_name, "w") as f:
        f.write(template_file)


if __name__ == "__main__":
    # backtest_cmd = "jesse backtest '2020-01-01' '2021-05-28'"
    backtest_cmd_template = "jesse backtest '{0}' '{1}'"
    symbols_dict = get_symbols()
    symbols, list_of_routes = build_routes(symbols_dict)
    rdf = results_helper.results_df

    symbol_route_list = list(zip(symbols, list_of_routes))
    random.shuffle(symbol_route_list)

    for symbol, routes in symbol_route_list:
        orig_symbol = symbol
        symbol = symbol.replace("-", "")
        """
        results_helper = ResultsHelper(strategy + suffix, use_s3=True)
        rdf = results_helper.results_df

        if not isinstance(rdf, type(None)) and len(rdf) and len(rdf[rdf["Symbol"] == symbol]) > 0:
            print(f" Skipping {symbol}")
            continue
        """
        earliest_date = get_earliest_date(orig_symbol)
        if earliest_date < absolute_earliest_date:
            backtest_cmd = backtest_cmd_template.format(str(absolute_earliest_date), str(to_date).split(" ")[0])
        else:
            backtest_cmd = backtest_cmd_template.format(str(earliest_date).split(" ")[0], str(to_date).split(" ")[0])

        write_template_file(routes)
        print(f"----------- Running backtest for {routes}")
        os.system(backtest_cmd)
        print(f"----------- Backtest complete")
