from symphony.tradingutils.timeframes import Timeframe
from symphony.tradingutils.currencies import Currency
from symphony.tradingutils.lots import Lot
from symphony.exceptions.risk_management_exception import RiskManagementError


class PositionSizer():
    """PositionSizer
    
    Base class for calculating position sizes in relative terms to supplied lots
    
    """

    def __init__(self, currency_pair: str, lot_size: int, account_denomination: str = "USD"):

        self.currency = Currency(currency_pair.upper())
        self.account_denomination = account_denomination
        self.lot = Lot(lot_size, account_denomination=account_denomination)

    def get_units(self,
                  risk_perc: float,
                  account_size: float,
                  stop_distance_in_pips: int,
                  price: float = -1.0
                  ) -> int:

        """get_units
        
        Dynamically calculate a position size given stop loss distance and an account percentage to risk
        
        Args:
            risk_perc (float): Account percentage at risk expressed as decimal
            account_size (float): Current account size
            stop_distance_in_pips (int): pips between open price and stoploss
            price (str, optional): Price to use for conversion if account currency is not the counter currency
        
        Returns:
            (int): Units
        
        Raises:
            RiskManagementError: For unknown error conditions
        """

        # https://www.babypips.com/learn/forex/calculating-position-sizes

        amount_risked = risk_perc * account_size

        known_units = 100 if "jpy" in self.currency.currency_pair.lower() else 10000

        units = -1.0
        if self.account_denomination == self.currency.counter:

            val_per_pip = amount_risked / stop_distance_in_pips
            units = val_per_pip * (self.lot.lot_size / self.lot.pip_value(self.currency.currency_pair))

        else:
            if price == -1.0:
                raise RiskManagementError(__name__ + ": Current price not provided when needed for sizing")

            units = (amount_risked / stop_distance_in_pips) * known_units
            if self.account_denomination == self.currency.base:
                units = units * price

            elif (self.account_denomination != self.currency.base) and (
                    self.account_denomination != self.currency.counter):
                conversion_pair = Currency(self.currency.get_conversion_pair(self.account_denomination))
                if self.account_denomination == conversion_pair.counter:
                    units = units / price
                elif self.account_denomination == conversion_pair.base:
                    units = units * price
                else:
                    RiskManagementError(__name__ + ": Unknown error in different currency")
            else:
                raise RiskManagementError(__name__ + ": Unknown error")

        return int(round(units, 0))

    def get_lots(self, units: int) -> float:
        """get_lots
        
        Returns lot size. Uses lot size definited in class instance
        
        Args:
            units (int): Base units
            
        Returns:
            (float): Lots
        """

        return round(units / self.lot.lot_size, 3)
