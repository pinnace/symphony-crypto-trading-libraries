from symphony.data_classes import PriceHistory
from symphony.abc import ClientABC
from symphony.enum import Timeframe
from alpha_vantage.timeseries import TimeSeries


class AlphaVantageClient(ClientABC):

    def __init__(self, api_key: str = "RT1BA5YM4GH5SFKB"):
        self.api_key: str = api_key
        self.ts_client = TimeSeries(key=self.api_key)

    def get(self, instrument: str, timeframe: Timeframe, num_bars: int) -> PriceHistory:
        pass
