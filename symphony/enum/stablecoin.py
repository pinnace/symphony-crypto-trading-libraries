from enum import Enum
from symphony.exceptions import EnumException
from typing import List, NewType, Union

StableCoin = NewType("StableCoin", Enum)


class StableCoins(Enum):
    """
    Stablecoin enum
    """

    USDT = "USDT"
    BUSD = "BUSD"
    USDC = "USDC"
    TUSD = "TUSD"
    PAX = "PAX"
    GUSD = "GUSD"

    @property
    def name(self) -> str:
        return self.value

    @property
    def stablecoin_strings(self) -> List[str]:
        return [c for c in dir(self) if not c.startswith("__")]

    @classmethod
    def str_to_stablecoin(cls, string: str) -> StableCoin:
        """
        Finds the Enum for a string representation of a stablecoin

        :param string: Stablecoin string
        :return: StableCoin
        :raises EnumException: If the string does not represent a stablecoin
        """
        for coin in cls:
            if coin.value == string:
                return coin
        raise EnumException(f"{string} is not a StableCoin")

    @classmethod
    def is_stablecoin(cls, string_or_stablecoin: Union[StableCoin, str]) -> bool:
        """
        Returns true if the supplied parameter is a stablecoin

        :param string_or_stablecoin: Stablecoin string or enum
        :return: True or False
        """
        if isinstance(string_or_stablecoin, cls):
            return True
        for coin in cls:
            if isinstance(string_or_stablecoin, str):
                if coin.value == string_or_stablecoin:
                    return True
        return False

