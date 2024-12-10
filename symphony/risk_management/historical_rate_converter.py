
import pandas as pd
from datetime import datetime as dt
from symphony.exceptions.risk_management_exception import RiskManagementError, HistDataMissing, UnknownProvider
from symphony.tradingutils.timeframes import Timeframe
from symphony.tradingutils.currencies import Currency
from symphony.tradingutils.lots import Lot
from symphony.risk_management.position_sizer import PositionSizer
from symphony.config.env import OANDA_DIR, FXCM_DIR, HISTDATA_DIR

class HistoricalRatesConverter(PositionSizer):
    """HistoricalRatesConverter
    
        For use in backtesting. If backtesting a non-USD pair,
        loads the appropriate historical data for on-demand conversion at a given
        point in time 
    """
    
    def __init__(self, provider: str, timeframe: str, currency_pair: str, lot_size: int, data_dir: str = "", account_denomination: str = "USD"):
        super().__init__(currency_pair, lot_size, account_denomination=account_denomination)
        
        self.provider = provider
        self.timeframe = Timeframe(timeframe)
        
        self.data_dir = ""
        # User specified data dir takes precedent
        if data_dir != "":
            self.data_dir = data_dir
        elif provider.lower() == 'oanda':
            self.data_dir = OANDA_DIR
        elif provider.lower() == 'fxcm':
            self.data_dir = FXCM_DIR
        elif provider.lower() == 'histdata':
            self.data_dir = HISTDATA_DIR
        else:
            raise UnknownProvider(__name__ + ": Unknown provider specified: {}".format(provider))
        
        
        self.df: pd.DataFrame = None
        
        currency_to_read: str = self.currency.currency_pair

        if self.account_denomination not in self.currency.currency_pair:
            conversion_pair = self.currency.get_conversion_pair(self.account_denomination)
            currency_to_read = conversion_pair
        self.conversion_pair = currency_to_read
            
        self.df = pd.read_csv(self.data_dir + currency_to_read + "/" + currency_to_read + self.timeframe.std + ".csv.gz")
        self.df.index = pd.to_datetime(self.df["Datetime"])
        
        
    def get_units(self, 
                    risk_perc: float, 
                    account_size: float, 
                    stop_distance_in_pips: int,
                    datetime: str,
                    datetime_fmt: str = "%Y-%m-%d %H:%M:%S",
                    ) -> float:
        """get_position_size
        
        Dynamically calculate a position size given stop loss distance and an account percentage to risk,
            but first grab the price from the historical record
        
        Args:
            risk_perc (float): Account percentage at risk expressed as decimal
            account_size (float): Current account size
            lot_size (float): The lot size being worked with
            stop_distance_in_pips (int): pips between open price and stoploss
            datetime (str): Current datetime
            datetime_format (str, opt): Format of datetime
        
        Returns:
            (float): Lot size
        
        Raises:
            RiskManagementError: For unknown error conditions
        """
        
        if self.account_denomination == self.currency.counter:
            return super().get_units(risk_perc, account_size, stop_distance_in_pips)
        else:
            dt_index = dt.strptime(datetime, datetime_fmt)
            try:
                index = self.df.index.get_loc(dt_index, method='nearest')
                price = self.df.iloc[index]["Close"]
            except:
                breakpoint()
            return super().get_units(risk_perc, account_size, stop_distance_in_pips, price=price)
        
        def get_lots(self, units: int) -> float:
            return super().get_lots(units)
        
        
        
        
        
        