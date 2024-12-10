from symphony.parser import ParserBaseClass
from symphony.exceptions import ParserClassException
from symphony.utils.time import to_unix_time
from symphony.abc import ParserABC
from symphony.enum import Column
from typing import List, Dict
from symphony.config import USE_MODIN
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


class CCXTParser(ParserABC, ParserBaseClass):

    def __init__(self):
        super().__init__()

    @staticmethod
    def parse(ccxt_client_response: List[List[float]]) -> pd.DataFrame:
        """
        Parser for results from ccxt_client.fetch_ohlcv()

        :param ccxt_client_response: CCXT output
        :return: Dataframe for PriceHistory
        """
        column_headers = [Column.TIMESTAMP, Column.OPEN, Column.HIGH, Column.LOW, Column.CLOSE, Column.VOLUME]
        df = pd.DataFrame(ccxt_client_response, columns=column_headers)
        df[Column.TIMESTAMP] = pd.to_datetime(df["timestamp"], unit='ms', utc=True)
        df = df.set_index(Column.TIMESTAMP)
        return df

