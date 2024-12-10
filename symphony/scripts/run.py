from symphony.data_classes import PriceHistory
from symphony.enum import Timeframe

if __name__ == "__main__":
    ph = PriceHistory()
    ph.timeframe = Timeframe.H1
