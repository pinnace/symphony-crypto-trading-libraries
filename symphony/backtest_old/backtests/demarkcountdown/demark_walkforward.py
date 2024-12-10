import backtrader as bt
import datetime
import itertools
import pandas as pd
import backtrader.feeds as btfeeds
from backtrader import TimeFrame
from symphony.backtest.strategies.demarkcountdown import DemarkCountdownStrategy
from symphony.backtest.analyzers.demark_analyzer import DemarkAnalyzer
import os


timeframe = "60"
        
instruments = ["EURUSD","GBPUSD","USDCHF", "AUDUSD", "EURGBP", "USDJPY"]
instruments = ["EURUSD"]
results = {
        "INSTRUMENT" : [],
        "TIMEFRAME" : [], 
        "TOTAL_TRADES" : [],
        "TOTAL_PROFIT" : [],
        "AVG_PROFIT" : [],
        "CCI" : [],
        "CCII" : [],
        "ADX_SIMPLE" : [],
        "ADX_LOOKBACK" : [],
        "RSI_RANGE" : [],
        "TRAILING_STOP" : [],
        "1ATR_STOP" : [],
        "FIB_TP" : [],
        
        #**{ instrument + "_" + timeframe + "_PROFIT" : [] for instrument in instruments }
}

NUM_PARAMS = 11
def strategy_callback(cb):
        breakpoint()
if __name__ == '__main__':
        """
                Hyperparameters:
                
                        CCI
                        CCII
                        ADX Simple (<45 at time of trade)
                        ADX Lookback   
                                - ADX was above 45 somewhere in the pattern and has moved below
                                - OR ADX was never above 45
                        RSI trading within range 33.33 < RSI_x < 66.66
                        Trailing Stop:
                                - Take distance between initial order and initial stop
                                - If price moves in our favor, move stop up, but keep same distance
                        1ATR Stop:
                                - push stoploss 1ATR
                        Fibonacci take profit:
                                - at 0.618 retracement
        """


        
        data_dir =  "./data/Oanda/Cleaned/"
        

        total_profit = 0
        
        for instrument in instruments:
                
                digits = 3 if "JPY" in instrument else 5
                position_size = 10 if "JPY" in instrument else 1000
                
                df = pd.read_csv(data_dir + instrument + "/" + instrument + timeframe + ".csv.gz")
                df.index = pd.to_datetime(df["Datetime"])  
                
                # Slices will be the 
                slices: list[tuple] = []
                window_size = round(len(df) / (NUM_PARAMS + 1))
                
                # Hold out frame for final validation
                hold_out_df = df[len(df) - window_size:]
                
                # Create the training sets from the training df
                training_df = df[:len(df) - window_size]
                
                # Use anchored walk forward method since the number of trades is relatively small
                
                cerebro = bt.Cerebro()#maxcpus=1)
                print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
                data = btfeeds.PandasData(
                        dataname=df,
                        open=1,
                        high=2,
                        low=3,
                        close=4,
                        volume=5,
                        openinterest=-1

                        )
                
                cerebro.adddata(data)
                output_path = None

                        
                #cerebro.addstrategy(DemarkCountdownStrategy, outputdir=output_path, instrument=instrument, digits=digits, position_size=position_size)
                
                """
                cerebro.optstrategy(DemarkCountdownStrategy,
                                outputdir=[output_path],
                                instrument=[instrument],
                                digits=[digits],
                                position_size=[position_size],
                                cancellation_qualifier_I=[False, True], 
                                cancellation_qualifier_II=[False, True],
                                adx_simple=[False, True],
                                adx_lookback=[False, True],
                                rsi_range=[False, True],
                                trailing_stop=[False, True],
                                atr_stop=[False,True],
                                fib_take_profit_level=[1.0, 0.618, 0.786],
                                close_on_conflict=[False, True]
                                )
                """
                
                """               
                cerebro.optstrategy(DemarkCountdownStrategy,
                                outputdir=[output_path],
                                instrument=[instrument],
                                digits=[digits],
                                position_size=[position_size],
                                cancellation_qualifier_I=[False, True], 
                                cancellation_qualifier_II=[False, True],
                                adx_simple=[False, True],
                                adx_lookback=[False, True],
                                rsi_range=[False, True],
                                trailing_stop=[False, True],
                                atr_stop=[False,True],
                                fib_take_profit_level=[1.0, 0.618, 0.786],
                                close_on_conflict=[False, True]
                                )
                """
                # Single
                
                cerebro.optstrategy(DemarkCountdownStrategy,
                                outputdir=[output_path],
                                instrument=[instrument],
                                digits=[digits],
                                position_size=[position_size],
                                cancellation_qualifier_I=[False], 
                                cancellation_qualifier_II=[False],
                                adx_simple=[False],
                                adx_lookback=[False],
                                rsi_range=[False],
                                trailing_stop=[True],
                                atr_stop=[False],
                                fib_take_profit_level=[1.0],
                                close_on_conflict=[False]
                                )
                
                
                
                cerebro.addanalyzer(DemarkAnalyzer, _name="demarkanalyzer")
                #cerebro.optcallback(strategy_callback)
                
                strats = cerebro.run()
                strats = list(itertools.chain(*strats))
                

                for strat in strats:
                        analysis = strat.analyzers.demarkanalyzer.get_analysis()
                        results["INSTRUMENT"].append(instrument)
                        results["TIMEFRAME"].append(timeframe)
                        results["TOTAL_TRADES"].append(analysis["TOTAL_TRADES"])
                        results["TOTAL_PROFIT"].append(analysis["TOTAL_PROFIT"])
                        avg_profit = 0 if not analysis["TOTAL_TRADES"] else round(float(analysis["TOTAL_PROFIT"]) / float(analysis["TOTAL_TRADES"]), 2)
                        results["AVG_PROFIT"].append(avg_profit)
                        results["CCI"].append(analysis["CCI"])
                        results["CCII"].append(analysis["CCII"])
                        results["ADX_SIMPLE"].append(analysis["ADX_SIMPLE"])
                        results["ADX_LOOKBACK"].append(analysis["ADX_LOOKBACK"])
                        results["RSI_RANGE"].append(analysis["RSI_RANGE"])
                        results["TRAILING_STOP"].append(analysis["TRAILING_STOP"])
                        results["1ATR_STOP"].append(analysis["1ATR_STOP"])
                        results["FIB_TP"].append(analysis["FIB_TP"])
                        
                #breakpoint()
                #print('{0}: Final Portfolio Value: {1:.2f}'.format(instrument, cerebro.broker.getvalue()))
                #total_profit += cerebro.broker.getvalue() - 10000
        result_df = pd.DataFrame(data=results)
        result_df = result_df.sort_values(by=['AVG_PROFIT'], ascending=False)
        breakpoint()