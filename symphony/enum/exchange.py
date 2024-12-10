from enum import Enum
from collections import namedtuple
from typing import List, NewType, Union
from .stablecoin import StableCoins

Exchange = namedtuple('Exchange', ['name', 'exchange_type', 'supported_stablecoins'])


class ExchangeType(Enum):
    """
    Class containing different types of exchanges
    """
    FX = "fx"
    CRYPTO = "crypto"
    EQUITIES = "equities"

class Exchanges(Enum):
    """
    Class containing constants for exchanges
    """

    BINANCE = Exchange(
        "binance",
        ExchangeType.CRYPTO,
        [StableCoins.USDT, StableCoins.BUSD, StableCoins.USDC, StableCoins.TUSD, StableCoins.PAX]
    )

    @property
    def name(self) -> str:
        return self.value.name

    @property
    def exchange_type(self) -> ExchangeType:
        return self.value.exchange_type

    @property
    def supported_stablecoins(self) -> List[StableCoins]:
        return self.value.supported_stablecoins









