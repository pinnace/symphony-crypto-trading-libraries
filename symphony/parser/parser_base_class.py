from symphony.data_classes import Candle
from typing import List


class ParserBaseClass:
    """
    ParserBaseClass:
        Implements all parsers as iterables
    """

    def __init__(self):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        pass
