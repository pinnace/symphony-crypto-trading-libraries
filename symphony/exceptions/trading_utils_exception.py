
class TradingUtilsError(Exception):
    """Generic error for TradingUtils classes and functions

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)
        
        

class UnknownTimeframe(Exception):
    """UnknownTimeframe error 

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)
        
class UnknownCurrency(Exception):
    """UnknownCurrency error 

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)
        
class UnknownLotName(Exception):
    """UnknownLotName error 

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)
        
class UnknownLotSize(Exception):
    """UnknownLotSize error 

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)
        
class UnquotablePipValue(Exception):
    """UnquotablePipValue error 

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)
        
class UnknownDigits(Exception):
    """UnknownDigits error 

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)

class UnknownConversionPair(Exception):
    """UnknownConversionPair error 

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)