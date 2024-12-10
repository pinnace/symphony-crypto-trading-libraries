import backtrader as bt
import datetime
import itertools
import pandas as pd
import os
import backtrader.feeds as btfeeds
import backtrader.analyzers as btanalyzers
from symphony.backtest.strategies.demarkcountdown.demark_countdown_strategy_v2 import DemarkCountdownStrategyV2
from symphony.backtest.analyzers.demark_analyzer import DemarkAnalyzer
from symphony.tradingutils.currencies import Currency
from symphony.tradingutils.timeframes import Timeframe
from symphony.config.env import OANDA_DIR, ROOT_DIR, BACKTEST_RESULTS_DIR

timeframes = ["1440", "240", "60"]
timeframes = [Timeframe(timeframe) for timeframe in timeframes]

instruments = ["EURUSD", "GBPUSD", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD"]
currencies = [Currency(instrument) for instrument in instruments]

HOLD_OUT_NUM = "6M"


def get_df(currency, timeframe) -> pd.DataFrame:
    df = pd.read_csv(OANDA_DIR + currency.currency_pair + "/" + currency.currency_pair + timeframe.std + ".csv.gz")
    df.index = pd.to_datetime(df["Datetime"])
    hold_out_df = df.last(HOLD_OUT_NUM)
    df = df.truncate(after=hold_out_df.index[0])
    return df

strategy_args = {"cancellation_qualifier_I" : True, "cancellation_qualifier_II" : True, "close_on_conflict" : True, "atr_stop" : True}

if __name__ == '__main__':
    results = {}
    for currency in currencies:
        for timeframe in timeframes:
            output_path = os.path.abspath(
                BACKTEST_RESULTS_DIR + "test_results/Oanda/OptimizedStrategy/TrailingStop/{}/{}/"
                    .format(currency.currency_pair, currency.currency_pair + timeframe.std
                            ))

            data = get_df(currency, timeframe)

            cerebro = bt.Cerebro()
            print('Starting Portfolio Value: {:.2f}'.format(cerebro.broker.getvalue()))
            data = btfeeds.PandasData(
                dataname=data,
                open=1,
                high=2,
                low=3,
                close=4,
                volume=5,
                openinterest=-1
            )
            cerebro.adddata(data)
            cerebro.broker.setcommission(margin=0.05)

            cerebro.addstrategy(DemarkCountdownStrategyV2,
                                instrument=currency.currency_pair,
                                timeframe=timeframe.std,
                                risk_perc=0.02,
                                outputdir=output_path,
                                **strategy_args
                                )
            cerebro.addanalyzer(btanalyzers.DrawDown, _name='drawdown')
            cerebro.addanalyzer(DemarkAnalyzer, _name="demarkanalyzer")
            strat = cerebro.run()

            thestrat = strat[0]
            analysis = thestrat.analyzers.demarkanalyzer.get_analysis()
            drawdown = thestrat.analyzers.drawdown.get_analysis()

            results[currency.currency_pair + str(timeframe.std)] = {
                "Profit": analysis["TOTAL_PROFIT"],
                "Drawdown": drawdown["max"]["drawdown"]
            }

    for key in results.keys():
        print("{}:\n\tP/L:{}\n\tDrawdown: {}".format(key, results[key]["Profit"], results[key]["Drawdown"]))
    breakpoint()



