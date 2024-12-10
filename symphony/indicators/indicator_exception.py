
class IndicatorError(Exception):
    """Generic error for Indicator classes

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)