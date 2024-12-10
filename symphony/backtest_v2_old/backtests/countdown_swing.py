import backtrader as bt
import backtrader.feeds as btfeeds
from symphony.enum import Timeframe
from symphony.data.archivers import BinanceArchiver
from symphony.config import USE_MODIN, USE_S3, S3_BUCKET, HISTORICAL_DATA_DIR
from symphony.backtest_v2.strategies import DemarkCountdownSwingStrategy

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


timeframe = Timeframe.H4
symbol = "ETHEUR"

if __name__ == '__main__':
    archiver = BinanceArchiver(HISTORICAL_DATA_DIR, use_s3=False)
    #archiver = BinanceArchiver(S3_BUCKET, use_s3=USE_S3)

    ph = archiver.read(symbol, timeframe)

    cerebro = bt.Cerebro()

    data = btfeeds.PandasData(
        dataname=ph.price_history,
        #dtformat=('%Y-%m-%d %H:%M:%SZ')
    )
    cerebro.adddata(data)

    cerebro.addstrategy(DemarkCountdownSwingStrategy, price_history=ph)
    strat = cerebro.run()