from pathlib import Path
from typing import List
from symphony.data_classes import Candle
from symphony.parser import ParserBaseClass
from symphony.exceptions import ParserClassException
from symphony.abc import ParserABC
from symphony.config import USE_MODIN
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


class CSVParser(ParserABC, ParserBaseClass):

    def __init__(self):
        super().__init__()

    def parse(self,
              filename: str,
              datetime_column: str = "datetime",
              column_names: List[str] = ["datetime", "open", "high", "low", "close", "volume"]
              ) -> pd.DataFrame:

        """
        Parses a CSV file into a dataframe

        :param filename: CSV file path
        :param datetime_column: Column containing dates or UNIX timestamps
        :param column_names: All column names
        :return: (pd.DataFrame)
        """

        file = Path(filename)
        if not file.is_file():
            raise ParserClassException(f"Cannot find file with path {filename}")

        df = pd.read_csv(file.resolve())
        df[datetime_column] = pd.to_datetime(df[datetime_column], unit='s')
        df.rename(columns={datetime_column: "timestamp"})
        df = df.set_index('timestamp')
        return df










