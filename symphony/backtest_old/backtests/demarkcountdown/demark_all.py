import backtrader as bt
import datetime
import pandas as pd
import backtrader.feeds as btfeeds
from backtrader import TimeFrame
from symphony.backtest.strategies.demarkcountdown import DemarkCountdownStrategy
import os


if __name__ == '__main__':
    
    data_dir = "../../../../data/Oanda/Cleaned/"
    timeframes = ["60"]
    instruments = ["EURGBP","EURUSD","GBPUSD","USDCAD","USDCHF", "AUDUSD", "USDJPY"]
    
    for instrument in instruments:
            digits = 3 if "JPY" in instrument else 5
            position_size = 10 if "JPY" in instrument else 1000

            for timeframe in timeframes:
                total_profit = 0

                # Break up the backtest to speed up
                df = pd.read_csv(data_dir + instrument + "/" + instrument + timeframe + ".csv.gz")
                df.index = pd.to_datetime(df["Datetime"])     
                slices = []
                window_size = round(len(df) / 10.)
                start_index = 0
                end_index = window_size

                while start_index < len(df):
                        s = df[start_index:end_index]
                        slices.append(s)
                        start_index = end_index - 200 # Accomodate patterns that may have been inbetween
                        end_index = start_index + window_size


                for slc in slices:
                        
                        print("{0}: Beginning process for slice between - {1} - {2} -".format(instrument, slc['Datetime'][0], slc['Datetime'].iloc[-1]))
                        cerebro = bt.Cerebro()
                        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

                        data = btfeeds.PandasData(
                                dataname=slc,
                                open=1,
                                high=2,
                                low=3,
                                close=4,
                                volume=5,
                                openinterest=-1

                        )
                        cerebro.adddata(data)
                        output_path = os.path.abspath("../results/test_results/Oanda/{}/{}/".format(instrument, instrument+timeframe))

                        
                        cerebro.addstrategy(DemarkCountdownStrategy, outputdir=output_path, instrument=instrument, digits=digits, position_size=position_size)
                        cerebro.run()
                        #cerebro.plot()
                        print("{0}: Finished process slice for - {1} - {2} -".format(instrument, slc['Datetime'][0], slc['Datetime'].iloc[-1]))
                        print('{0}: Final Portfolio Value: {1:.2f}'.format(instrument, cerebro.broker.getvalue()))
                        total_profit += cerebro.broker.getvalue() - 10000
                print("[ {} : {} ] Total profit for backtest: {}".format(instrument, timeframe, total_profit))





