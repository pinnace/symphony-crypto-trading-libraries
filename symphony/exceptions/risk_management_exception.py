
class RiskManagementError(Exception):
    """Generic error for Risk Management classes

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)
        

class HistDataMissing(Exception):
    """Generic error for HistoricalRatesConverter classes

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)
        
class UnknownProvider(Exception):
    """Generic error for UnknownProvider 

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)