import backtrader as bt
import datetime
import backtrader.feeds as btfeeds
from backtrader import TimeFrame
from symphony.backtest.strategies.demarkcountdown import DemarkCountdownStrategy
import os


if __name__ == '__main__':
    cerebro = bt.Cerebro()
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    data = btfeeds.GenericCSVData(
            dataname='../data/test_data/EUR_USD_1H.csv',

            datetime=0,
            volume=1,
            open=2,
            high=3,
            low=4,
            close=5,
            openinterest=-1

    )
    cerebro.adddata(data)
    output_path = os.path.abspath("../results/test_results/")
    cerebro.addstrategy(DemarkCountdownStrategy, outputdir=output_path, instrument="EURUSD")
    cerebro.run()

    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())