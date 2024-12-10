
class TestingError(Exception):
    """Generic error for tests 

    Args:
        message (str): Error message

    """
    def __init__(self,message):
        super().__init__(message)