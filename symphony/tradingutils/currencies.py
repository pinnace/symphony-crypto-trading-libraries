from symphony.exceptions.trading_utils_exception import UnknownCurrency, UnknownConversionPair



class Currency():
    """Currency
    
    The currency class will enrich information about a given
    currency by exposing certain derived properties
    
    Args:
        currency_pair (str): The currency pair
    """
    
    currency_pairs = [
        "AUDCAD",
        "AUDCHF",
        "AUDJPY",
        "AUDNZD",
        "AUDUSD",
        "CADCHF",
        "EURAUD",
        "EURCHF",
        "EURGBP",
        "EURJPY",
        "EURUSD",
        "GBPCHF",
        "GBPJPY",
        "GBPNZD",
        "GBPUSD",
        "NZDCAD",
        "NZDCHF",
        "NZDJPY",
        "NZDUSD",
        "USDCAD",
        "USDCHF",
        "USDHKD",
        "USDJPY",
        "CHFJPY",
        "GBPCAD"
    ]
    
    def __init__(self, currency_pair: str):
        super().__init__()
        if currency_pair not in Currency.currency_pairs:
            raise UnknownCurrency(__name__ + ": Unknown currency: {}".format(currency_pair))
        self.currency_pair = currency_pair
        self.base = self.currency_pair[:3]
        self.counter = self.currency_pair[3:]
        self.digits = 3 if "jpy" in currency_pair.lower() else 5     
    
    
    def get_conversion_pair(self, account_denomination: str) -> str:
        """get_conversion_pair
        
        Identify the conversion pair if the account denominated currency is not in the currently
            traded pair
            
        Args:
            account_denomination (str): The currency denomination of the account
        
        Returns:
            (str): Identified conversion pair
            
        Raises:
            UnknownConversionPair: If the conversion pair cannot be identified
        """
        
        
        if self.counter + account_denomination in Currency.currency_pairs:
            return self.counter + account_denomination
        elif account_denomination + self.counter in Currency.currency_pairs:
            return account_denomination + self.counter
        else:
            raise UnknownConversionPair(__name__ + ": Could not find conversion pair for {} (counter) and {} (account_denom)".format(self.counter, account_denomination))
        