class BacktesterError(Exception):
    """Generic error for Backtester

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)