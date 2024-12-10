
from symphony.exceptions.trading_utils_exception import UnknownTimeframe


class Timeframe():
    """Timeframe
    
    Provides various enrichment and additional methods for timeframes (e.g. string rep)
    
    Args:
        timeframe (str): The timeframe being traded
    
    """
    
    timeframe_mapping = {
        "M1" : "1",
        "M5" : "5",
        "M15" : "15",
        "M30" : "30",
        "H1" : "60",
        "H4" : "240",
        "D1" : "1440"
    }
    
    def __init__(self, timeframe: str):
        if timeframe in Timeframe.timeframe_mapping.keys():
            self.std = Timeframe.timeframe_mapping[timeframe]
            self.str = timeframe
        elif timeframe in Timeframe.timeframe_mapping.values():
            self.std = timeframe
            inverse = {v: k for k, v in Timeframe.timeframe_mapping.items()}
            self.str = inverse[timeframe]
        else:
            raise UnknownTimeframe("Unknown timeframe: {}".format(timeframe))

    
        
        
    