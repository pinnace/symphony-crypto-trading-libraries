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
from symphony.config.env import OANDA_DIR, ROOT_DIR

timeframe = Timeframe("60")
instruments = ["EURUSD", "GBPUSD", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD"]
#instruments = ["USDCHF"]
currencies = [Currency(instrument) for instrument in instruments]
HOLD_OUT_NUM = "6M"
results_proto = {
    "INSTRUMENT": [],
    "TIMEFRAME": [],
    "POSSIBLE_BUY_TRADES": [],
    "POSSIBLE_SELL_TRADES": [],
    "TOTAL_BUY_TRADES": [],
    "TOTAL_SELL_TRADES": [],
    "PROFITABLE_BUY_TRADES": [],
    "PROFITABLE_SELL_TRADES": [],
    "TOTAL_PROFIT": [],
    "AVG_PROFIT": [],
    "PERC_PROFITABLE": [],
    "PERC_TRADES_TAKEN": [],
    "SHARPE_RATIO": [],
    "DRAWDOWN": [],
    "MAX_DRAWDOWN": [],
    "CCI": [],
    "CCII": [],
    "ADX_SIMPLE": [],
    "ADX_LOOKBACK": [],
    "RSI_RANGE": [],
    "CLOSE_ON_CONFLICT": [],
    "TRAILING_STOP": [],
    "1ATR_STOP": [],
    "FIB_TP": [],

    # **{ instrument + "_" + timeframe + "_PROFIT" : [] for instrument in instruments }
}


def get_df() -> pd.DataFrame:
    df = pd.read_csv(OANDA_DIR + currency.currency_pair + "/" + currency.currency_pair + timeframe.std + ".csv.gz")
    df.index = pd.to_datetime(df["Datetime"])
    hold_out_df = df.last(HOLD_OUT_NUM)
    df = df.truncate(after=hold_out_df.index[0])
    return df


def populate_results(results, cerebro_strats, currency_pair, tf) -> dict:
    for strat in cerebro_strats:
        analysis = strat.analyzers.demarkanalyzer.get_analysis()
        sharpe = strat.analyzers.sharpe.get_analysis()
        drawdown = strat.analyzers.drawdown.get_analysis()

        # Get percent of trades that were profitable
        if analysis["TOTAL_BUY_TRADES"] + analysis["TOTAL_SELL_TRADES"] > 0:
            perc_profitable = round((analysis["PROFITABLE_BUY_TRADES"] + analysis["PROFITABLE_SELL_TRADES"]) /
                                    (analysis["TOTAL_BUY_TRADES"] + analysis["TOTAL_SELL_TRADES"]), 2)
        else:
            perc_profitable = 0

        # Get percent of trades taken
        total_possible = analysis["POSSIBLE_BUY_TRADES"] + analysis["POSSIBLE_SELL_TRADES"]
        if total_possible > 0:
            trades_taken = analysis["TOTAL_BUY_TRADES"] + analysis["TOTAL_SELL_TRADES"]
            perc_taken = round(trades_taken / total_possible, 2)
        else:
            perc_taken = 0

        results["INSTRUMENT"].append(currency_pair)
        results["TIMEFRAME"].append(tf)
        results["POSSIBLE_BUY_TRADES"].append(analysis["POSSIBLE_BUY_TRADES"])
        results["POSSIBLE_SELL_TRADES"].append(analysis["POSSIBLE_SELL_TRADES"])
        results["TOTAL_BUY_TRADES"].append(analysis["TOTAL_BUY_TRADES"])
        results["TOTAL_SELL_TRADES"].append(analysis["TOTAL_SELL_TRADES"])
        results["PROFITABLE_BUY_TRADES"].append(analysis["PROFITABLE_BUY_TRADES"])
        results["PROFITABLE_SELL_TRADES"].append(analysis["PROFITABLE_SELL_TRADES"])
        results["TOTAL_PROFIT"].append(analysis["TOTAL_PROFIT"])
        avg_profit = 0 if not analysis["TOTAL_BUY_TRADES"] and not analysis["TOTAL_SELL_TRADES"] else round(
            float(analysis["TOTAL_PROFIT"]) / float(analysis["TOTAL_BUY_TRADES"] + analysis["TOTAL_SELL_TRADES"]),
            2)
        results["AVG_PROFIT"].append(avg_profit)
        results["PERC_PROFITABLE"].append(perc_profitable)
        results["PERC_TRADES_TAKEN"].append(perc_taken)
        results["SHARPE_RATIO"].append(sharpe["sharperatio"])
        results["DRAWDOWN"].append(drawdown["drawdown"])
        results["MAX_DRAWDOWN"].append(drawdown["max"]["drawdown"])
        results["CCI"].append(analysis["CCI"])
        results["CCII"].append(analysis["CCII"])
        results["ADX_SIMPLE"].append(analysis["ADX_SIMPLE"])
        results["ADX_LOOKBACK"].append(analysis["ADX_LOOKBACK"])
        results["RSI_RANGE"].append(analysis["RSI_RANGE"])
        results["CLOSE_ON_CONFLICT"].append(analysis["CLOSE_ON_CONFLICT"])
        results["TRAILING_STOP"].append(analysis["TRAILING_STOP"])
        results["1ATR_STOP"].append(analysis["1ATR_STOP"])
        results["FIB_TP"].append(analysis["FIB_TP"])
    return results


if __name__ == '__main__':

    for currency in currencies:
        results_dir = "symphony/backtest/backtests/results/optimization_results/"
        outfile = results_dir + currency.currency_pair + timeframe.std + "_results.csv"
        if os.path.exists(outfile):
            continue
        results = results_proto.copy()
        data = get_df()

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
        cerebro.optstrategy(DemarkCountdownStrategyV2,
                            instrument=[currency.currency_pair],
                            timeframe=[timeframe.std],
                            risk_perc=[0.02],
                            cancellation_qualifier_I=[False, True],
                            cancellation_qualifier_II=[False, True],
                            adx_simple=[False, True],
                            adx_lookback=[False, True],
                            rsi_range=[False, True],
                            trailing_stop=[False, True],
                            atr_stop=[False, True],
                            fib_take_profit_level=[1.0],
                            close_on_conflict=[False, True]
                            )
        # cerebro.addstrategy(DemarkCountdownStrategyV2, currency.currency_pair, timeframe.std)
        cerebro.addanalyzer(DemarkAnalyzer, _name="demarkanalyzer")
        cerebro.addanalyzer(btanalyzers.SharpeRatio_A, _name='sharpe')
        cerebro.addanalyzer(btanalyzers.DrawDown, _name='drawdown')
        strats = cerebro.run()

        strats = list(itertools.chain(*strats))

        results = populate_results(results, strats, currency.currency_pair, timeframe.std)
        result_df = pd.DataFrame(data=results)
        result_df = result_df.sort_values(by=['TOTAL_PROFIT'], ascending=False)
        result_df.to_csv(outfile, index=False)
        results.clear()
    breakpoint()
