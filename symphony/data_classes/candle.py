import pandas

class Candle:
    """
    Candle:
        Represents a single candle.

    Args:
        open (float): Open price
        high (float): High price
        low (float): Low price
        close (float): Close price
        volume (int): Volume
        time (datetime.datetime): Candle time
    """
    def __init__(self,
                 open: float = -1,
                 high: float = -1,
                 low: float = -1,
                 close: float = -1,
                 volume: int = -1,
                 timestamp: pandas.Timestamp = None):

        self.open: float = open
        self.high: float = high
        self.low: float = low
        self.close: float = close
        self.volume: int = volume
        self.timestamp: pandas.Timestamp = timestamp

    @property
    def open(self) -> float:
        return self.__open

    @open.setter
    def open(self, open: float):
        self.__open = open

    @property
    def high(self) -> float:
        return self.__high

    @high.setter
    def high(self, high: float):
        self.__high = open

    @property
    def low(self) -> float:
        return self.__low

    @low.setter
    def low(self, low: float):
        self.__low = low

    @property
    def close(self) -> float:
        return self.__close

    @close.setter
    def close(self, close: float):
        self.__close = close

    @property
    def volume(self) -> int:
        return self.__volume

    @volume.setter
    def volume(self, volume: int):
        self.__volume = volume

    @property
    def timestamp(self) -> pandas.Timestamp:
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, timestamp: pandas.Timestamp):
        self.__timestamp = timestamp
