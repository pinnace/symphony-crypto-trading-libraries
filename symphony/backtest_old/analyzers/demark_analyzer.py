from backtrader import Analyzer


class DemarkAnalyzer(Analyzer):

    def __init__(self):
        self.analysis = {}

    def start(self):
        # Not needed ... but could be used
        pass

    def next(self):
        # Not needed ... but could be used
        pass

    def stop(self):
        self.analysis = {
            "TOTAL_PROFIT": self.strategy.total_profit,
            "POSSIBLE_BUY_TRADES": self.strategy.possible_buy_trades,
            "POSSIBLE_SELL_TRADES": self.strategy.possible_sell_trades,
            "TOTAL_BUY_TRADES": self.strategy.buy_trade_count,
            "TOTAL_SELL_TRADES": self.strategy.sell_trade_count,
            "PROFITABLE_BUY_TRADES": self.strategy.profitable_buy_trade_count,
            "PROFITABLE_SELL_TRADES": self.strategy.profitable_sell_trade_count,
            "CCI": self.strategy.cancellation_qualifier_I,
            "CCII": self.strategy.cancellation_qualifier_II,
            "ADX_SIMPLE": self.strategy.adx_simple,
            "ADX_LOOKBACK": self.strategy.adx_lookback,
            "RSI_RANGE": self.strategy.rsi_range,
            "CLOSE_ON_CONFLICT": self.strategy.close_on_conflict,
            "TRAILING_STOP": self.strategy.trailing_stop,
            "1ATR_STOP": self.strategy.atr_stop,
            "FIB_TP": self.strategy.fib_take_profit_level
        }

    def get_analysis(self):
        return self.analysis
