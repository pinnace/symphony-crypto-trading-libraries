from symphony.data_classes import PriceHistory


class Flow:
    """
    Flow:
        Represents a chart. Contains the price history, instrument metadata, and indicators.
        Index 0 is always most recent

    Args:
        price_history (symphony.data_classes.PriceHistory): Price history object
    """
    def __init__(self, price_history: PriceHistory = None):

        self.price_history: PriceHistory = price_history

    @property
    def price_history(self):
        return self.__price_history

    @price_history.setter
    def price_history(self, price_history: PriceHistory):
        self.__price_history = price_history


