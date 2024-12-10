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


class BinanceParser(ParserABC, ParserBaseClass):

    def __init__(self):
        super().__init__()

    #TODO: Accept mapping of response column to Column
    @staticmethod
    def parse(
              binance_client_response: List[List],
              columns_to_keep: List[str] =["timestamp", "open", "high", "low", "close", "volume"]
              ) -> pd.DataFrame:
        """
        Parses the response from the binance client

        :param binance_client_response: (`List[List]`) Raw response from binance client
        :param columns_to_keep: (`List[str]`) Columns we are interested in
        :return:
        """
        columns = [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "closetime",
            "quote_asset_volume",
            "num_trades",
            "taker_buy_base",
            "take_buy_quote",
            "ignore"
        ]
        df = pd.DataFrame(binance_client_response, columns=columns)
        # Convert UNIX time to datetime and set as index
        df[Column.TIMESTAMP] = pd.to_datetime(df["timestamp"], unit='ms', utc=True)
        df["closetime"] = pd.to_datetime(df["closetime"], unit='ms', utc=True)
        df = df.set_index(Column.TIMESTAMP)
        df[[Column.OPEN, Column.HIGH, Column.LOW, Column.CLOSE, Column.VOLUME]] = \
            df[[Column.OPEN, Column.HIGH, Column.LOW, Column.CLOSE, Column.VOLUME]].apply(pd.to_numeric)

        # Remove unwanted columns
        diff = list(set(columns) - set(columns_to_keep))
        df = df.drop(diff, axis=1)
        return df

    @staticmethod
    def parse_websocket_message(msg: Dict[str, Dict[str, str]]) -> Dict[pd.Timestamp, Dict[str, str]]:
        """
        Parses Binance kline websocket message.
        Returns
        {
            Timestamp(...): {
                "open", ...
            }
        }
        :param msg: Websocket message
        :return: Row for PriceHistory.append
        """
        row = {}
        timestamp = pd.Timestamp(msg["k"]["t"], unit='ms', tz='UTC')
        row[timestamp] = {
            "open": float(msg["k"]["o"]),
            "high": float(msg["k"]["h"]),
            "low": float(msg["k"]["l"]),
            "close": float(msg["k"]["c"]),
            "volume": float(msg["k"]["v"]),
        }
        return row

