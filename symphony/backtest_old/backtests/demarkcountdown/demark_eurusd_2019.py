import backtrader as bt
import datetime
import backtrader.feeds as btfeeds
from backtrader import TimeFrame
from symphony.backtest.strategies.demarkcountdown import DemarkCountdownStrategy
import os


if __name__ == '__main__':
    cerebro = bt.Cerebro()
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    time_dir = "ALL"
    dataset_name = "EURUSD60"

    data = btfeeds.GenericCSVData(
            dtformat='%Y-%m-%d %H:%M:%S',
            dataname='../../../../data/EURUSD/{}/{}-converted.csv'.format(time_dir, dataset_name),

            datetime=0,
            volume=1,
            open=2,
            high=3,
            low=4,
            close=5,
            openinterest=-1

    )
    cerebro.adddata(data)
    output_path = os.path.abspath("../results/test_results/{}/{}/".format(time_dir, dataset_name))
    cerebro.addstrategy(DemarkCountdownStrategy, outputdir=output_path, instrument="EURUSD", digits=5)
    cerebro.run()
    cerebro.plot()

    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())