import backtrader as bt
from symphony.risk_management.historical_rate_converter import HistoricalRatesConverter
from symphony.tradingutils.currencies import Currency
from symphony.tradingutils.timeframes import Timeframe

class ForexSizer(bt.Sizer):

    def __init__(self, currency, timeframe, risk_perc=0.02):
        self.currency = Currency(currency)
        self.timeframe = Timeframe(timeframe)
        self.risk_perc = risk_perc
        self.hrc = HistoricalRatesConverter('Oanda', self.timeframe.std, self.currency.currency_pair, 10000)

    def _getsizing(self, comminfo, cash, data, isbuy):
        size = self.hrc.get_units(self.risk_perc, cash, )
        breakpoint()
        return 10000
