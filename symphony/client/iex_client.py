from symphony.data_classes import PriceHistory
from symphony.abc import ClientABC
from symphony.enum import Timeframe
import pandas as pd
from iexfinance.refdata import get_symbols


class IEXClient(ClientABC):
    """

    def __init__(self, public_key: str = "pk_b6cc215f52c843e086cdbeb7977e6eb6", secret_key: str = "sk_84b3426a8ee24e6da4db508d07ada98e"):
        self.public_key: str = public_key
        self.secret_key: str = secret_key


    def get(self, instrument: str, timeframe: Timeframe, num_bars: int) -> PriceHistory:
        pass

    def get_all_symbols(self, live=False) -> pd.DataFrame:
        if live:
            symbols = get_symbols(token=self.secret_key)

    """
