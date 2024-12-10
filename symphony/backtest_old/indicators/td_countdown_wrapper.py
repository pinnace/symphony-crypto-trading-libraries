from symphony.indicators import Indicators
import backtrader as bt
from symphony.indicators import Indicators, IndicatorKit
from symphony.schema import SchemaKit

class DemarkCountdownBtIndicatorWrapper(bt.Indicator):
    lines = ("bearish_price_flips", "buy_setups", "buy_setups_true_end", "perfect_buy_setups", "tdst_resistance", \
        "active_buy_setups", "buy_countdowns","bullish_price_flips", "sell_setups", "sell_setups_true_end", "perfect_sell_setups", \
            "tdst_support", "active_sell_setups", "sell_countdowns", "tradeable_buy_setups", "tradeable_sell_setups", )
    
    # Price flip is 6 bars
    params = (('period', 6),)

    def __init__(self, cancellation_qualifier_I: bool = False, cancellation_qualifier_II: bool = False):
        #self.addminperiod(self.params.period)
        self.flow = SchemaKit.standard_flow()
        
        length = self.data.close.buflen() 
        opens = self.data.open.get(ago=-1, size=length)
        highs = self.data.high.get(ago=-1, size=length)
        lows = self.data.low.get(ago=-1, size=length)
        closes = self.data.close.get(ago=-1, size=length)
        volumes = self.data.volume.get(ago=-1, size=length)
        self.flow["price_history"] = [[o, h, l, c, v] for o,h,l,c,v in zip(opens,highs,lows,closes,volumes)]
        self.flow["price_history"].append([self.data.open[0], self.data.high[0], self.data.low[0], self.data.close[0], self.data.volume[0]])
        self.demark_channels = Indicators.td_countdown(self.flow, cancellation_qualifier_I=cancellation_qualifier_I, cancellation_qualifier_II=cancellation_qualifier_II)
        self.count = 0

        """
        self.l.buy_countdowns = self.data.close
        for i, val in enumerate(demark_channels["buy_countdowns"][::-1]):
            breakpoint()
            self.lines.buy_countdowns[i] = val
        """
        for key in self.demark_channels.keys():
            self.demark_channels[key] = IndicatorKit.pad_to_length(self.demark_channels[key], length)

    def next(self):
        self.buy_countdowns[0] = self.demark_channels["buy_countdowns"][self.count]
        for key in self.demark_channels.keys():
            try:
                getattr(self.lines, key)[0] = self.demark_channels[key][self.count]
            except IndexError:
                #breakpoint()
                pass

        self.count += 1
        """
        IndicatorKit.append_ohlc_into_flow(self.flow, self.data.open[0], self.data.high[0], self.data.low[0], self.data.close[0], self.data.volume[0] )
        
        demark_channels = Indicators.td_countdown(self.flow)
        self.buy_countdowns[0] = demark_channels["buy_countdowns"][-1]
        for line in demark_channels.keys():
            if line != "buy_countdowns":
                attr = getattr(self.lines, line)
                attr = demark_channels[line]
        """
        pass
