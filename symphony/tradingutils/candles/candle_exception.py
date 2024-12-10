
class CandleError(Exception):
    """Generic error for Candle classes

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)