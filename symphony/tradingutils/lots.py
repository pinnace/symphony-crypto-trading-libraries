from symphony.exceptions.trading_utils_exception import UnknownLotName, UnknownLotSize, UnquotablePipValue, UnknownDigits
from symphony.tradingutils.currencies import Currency


class Lot():
    """Lot
    
    Enriches and provides utility method for lot sizes (e.g. pip value)
    
    Args:
        lot_size (int): The size of your account's lot
        account_denomination (str, opt): Optional account denomination. Default is USD
    
    """
    lots_mapping = {
        'standard' : 100000,
        'mini' : 10000,
        'micro' : 1000,
        'nano' : 100
    }
    
    # https://www.babypips.com/learn/forex/lots-leverage-and-profit-and-loss
    
    def __init__(self, lot_size: int, account_denomination: str = "USD"):
        self.lot_name = Lot.lot_name(lot_size)
        self.lot_size = lot_size
        self.worth_per_pip_move = self.__worth_of_pip_move()
        self.account_denomination = account_denomination
        
    def pip_value(self, currency_pair: str, price: float = -1.0) -> float:
        """pip_value
        
        Returns the value of a pip based on the lot size, account denomination, and if necessary, current price
        
        Args:
            currency_pair (str): The currently traded pair.
            price (:obj:`str`, optional): The current price of the pair if the counter currency is not the denominated currency
            
        Returns:
            float: Per pip value based on price, lot size, and account denomination
            
        Raises:
            UnquotablePipValue: If a price is not supplied when the traded currency does not have the account denominated
                                    currency as its counter
        """
        
        currency = Currency(currency_pair)
        
        if (currency.counter != self.account_denomination) and price == -1.0:
            raise UnquotablePipValue(__name__ + ": Cannot quote {} because the price is undefined".format(currency_pair))
        
        if currency.counter == self.account_denomination:
            return round(float(self.lot_size) / 10000., 2)
        elif currency.base == self.account_denomination:
            base_pip_size = Lot.pip_size(currency.digits)
            return round(( base_pip_size / price ) * self.lot_size, 2)
        else:
            raise Exception()
        
    
    @staticmethod
    def lot_name(lot_size: int) -> str:
        """lot_name
        
        Args:
            lot_size (int): The size of the lot. Values: 100000, 10000, 1000, 100
        
        Returns:
            (str): The name of the lot size
        
        Raises:
            UnknownLotSize: If the lot size is unknown
        """
        inverse = {v: k for k, v in Lot.lots_mapping.items()}
        if lot_size in inverse.keys():
            return inverse[lot_size]
        else:
            raise UnknownLotSize(__name__ + ": Unknown lot size: {}".format(lot_size))
    
    @staticmethod
    def lot_size(lot_name: str) -> int:
        """lot_size
        
        Args:
            lot_name (str): The name of the lot size. Values: 'standard', 'mini', 'micro', 'nano'
        
        Returns:
            (int): The size of the lot
        
        Raises:
            UnknownLotName: If the lot name is unknown
        """
        if lot_name.lower() in lots_mapping.keys():
            return lots_mapping[lot_name]
        else:
            raise UnknownLotName(__name__ + ": Unknown lot name: {}".format(lot_name))
        
    @staticmethod
    def pip_size(digits: int) -> float:
        """pip_size
        
        Args:
            digits (int): Either 3 (for JPY pairs) or 5
        
        Returns:
            float:The base pip value (either 0.01 or 0.0001)
        
        Raises:
            UnknownDigits: If digits neither 3 nor 5

        """
        
        if digits != 3 and digits != 5:
            raise UnknownDigits(__name__ + ": Unknown number of digits: {}".format(digits))
        
        return 0.01 if digits == 3 else 0.0001
    
    def __worth_of_pip_move(self) -> float:
        """__worth_of_pip_move
        
        Determine the value of a 1 pip move for 1 lot
        
        Returns:
            The value of the 1 pip move, denominated in account currency
        """
        if self.lot_size == 100000:
            return 10.0
        elif self.lot_size == 10000:
            return 1.0
        elif self.lot_size == 1000:
            return 0.1
        elif self.lot_size == 100:
            return 0.01
        else:
            raise UnknownLotSize(__name__ + ": Unknown lot size when determining worth of pip")
        
        