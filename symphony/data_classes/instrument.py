from symphony.exceptions import DataClassException
from symphony.enum import Exchange, StableCoin
from typing import List, Union, Type, Optional
from dataclasses import dataclass


@dataclass
class Instrument:
    """
    Instrument:

        Data class for a financial instrument
    """

    def __init__(self,
                 symbol: str = "",
                 digits: int = -1,
                 exchange: Optional[Exchange] = None,
                 is_currency: Optional[bool] = False,
                 base_asset: Optional[str] = None,
                 quote_asset: Optional[str] = None,
                 margin_allowed: Optional[bool] = False,
                 isolated_margin_allowed: Optional[bool] = False,
                 isolated_margin_account_created: Optional[bool] = False,
                 isolated_margin_ratio: Optional[int] = 0,
                 oco_allowed: Optional[bool] = False,
                 min_quantity: Optional[float] = 0.0,
                 max_quantity: Optional[float] = 0.0,
                 step_size: Optional[float] = 0.0
                 ):
        """
        __init__:

        :param symbol: String representation of the financial instrument
        :param digits: Precision
        :param exchange: Exchange
        :param is_currency: True if currency or cryptocurrency
        :param base_asset: If currency, the base asset
        :param quote_asset: If currency, the quote asset
        :param margin_allowed: If margin allowed
        :param isolated_margin_allowed: If isolated margin allowed
        :param isolated_margin_account_created: If isolated margin account has been created
        :param isolated_margin_ratio: The margin ratio if isolated margin allowed
        :param min_quantity: Minimum order quantity
        :param max_quantity: Maximum order quantity
        :param step_size: Lot rounding size
        """
        self.symbol: str = symbol
        self.digits: int = digits
        self.exchange: Exchange = exchange
        self.is_currency: bool = is_currency
        self.base_asset: str = base_asset
        self.quote_asset: str = quote_asset
        self.margin_allowed: bool = margin_allowed
        self.isolated_margin_allowed: bool = isolated_margin_allowed
        self.isolated_margin_account_created: bool = isolated_margin_account_created
        self.isolated_margin_ratio: int = isolated_margin_ratio
        self.oco_allowed: bool = oco_allowed
        self.min_quantity: float = min_quantity
        self.max_quantity: float = max_quantity
        self.step_size: float = step_size

    def __eq__(self, other):
        if not isinstance(other, Instrument):
            return NotImplemented

        return self.symbol == other.symbol and self.digits == other.digits and self.exchange == other.exchange and \
               self.is_currency == other.is_currency and self.base_asset == other.base_asset and \
               self.quote_asset == other.quote_asset and \
               self.margin_allowed == other.margin_allowed and \
               self.isolated_margin_allowed == other.isolated_margin_allowed and \
               self.oco_allowed == other.oco_allowed and self.min_quantity == other.min_quantity and \
               self.max_quantity == other.max_quantity

    def __repr__(self):
        return f"Instrument / {self.symbol} / {self.exchange.name.capitalize()}"

    @property
    def name(self) -> str:
        return repr(self)

    @property
    def symbol(self) -> str:
        return self.__symbol

    @symbol.setter
    def symbol(self, symbol: str):
        self.__symbol = symbol

    @property
    def digits(self) -> int:
        return self.__digits

    @digits.setter
    def digits(self, digits: int):
        if not isinstance(digits, int):
            raise DataClassException(f"{digits} is not an integer")
        self.__digits = digits

    @property
    def exchange(self) -> Exchange:
        return self.__exchange

    @exchange.setter
    def exchange(self, exchange: Exchange):
        self.__exchange = exchange

    @property
    def is_currency(self) -> bool:
        return self.__is_currency

    @is_currency.setter
    def is_currency(self, is_currency: bool):
        self.__is_currency = is_currency

    @property
    def base_asset(self) -> str:
        return self.__base_asset

    @base_asset.setter
    def base_asset(self, base_asset: str):
        self.__base_asset = base_asset

    @property
    def quote_asset(self) -> str:
        return self.__quote_asset

    @quote_asset.setter
    def quote_asset(self, quote_asset: str):
        self.__quote_asset = quote_asset

    # Aliases
    @property
    def base_currency(self) -> str:
        return self.__base_asset

    @base_currency.setter
    def base_currency(self, base_currency: str):
        self.__base_asset = base_currency

    @property
    def counter_currency(self) -> str:
        return self.__quote_asset

    @counter_currency.setter
    def counter_currency(self, counter_currency: str):
        self.__quote_asset = counter_currency

    @property
    def margin_allowed(self) -> bool:
        return self.__margin_allowed

    @margin_allowed.setter
    def margin_allowed(self, margin_allowed: bool):
        self.__margin_allowed = margin_allowed

    @property
    def isolated_margin_allowed(self) -> bool:
        return self.__isolated_margin_allowed

    @isolated_margin_allowed.setter
    def isolated_margin_allowed(self, isolated_margin_allowed: bool):
        self.__isolated_margin_allowed = isolated_margin_allowed

    @property
    def isolated_margin_account_created(self) -> bool:
        return self.__isolated_margin_account_created

    @isolated_margin_account_created.setter
    def isolated_margin_account_created(self, isolated_margin_account_created: bool):
        self.__isolated_margin_account_created = isolated_margin_account_created

    @property
    def isolated_margin_ratio(self) -> int:
        return self.__isolated_margin_ratio

    @isolated_margin_ratio.setter
    def isolated_margin_ratio(self, isolated_margin_ratio: int):
        self.__isolated_margin_ratio = isolated_margin_ratio

    @property
    def min_quantity(self) -> float:
        return self.__min_quantity

    @min_quantity.setter
    def min_quantity(self, min_quantity: float):
        self.__min_quantity = min_quantity

    @property
    def max_quantity(self) -> float:
        return self.__max_quantity

    @max_quantity.setter
    def max_quantity(self, max_quantity: float):
        self.__max_quantity = max_quantity

    @property
    def step_size(self) -> float:
        return self.__step_size

    @step_size.setter
    def step_size(self, step_size: float):
        self.__step_size = step_size

    @property
    def oco_allowed(self) -> bool:
        return self.__oco_allowed

    @oco_allowed.setter
    def oco_allowed(self, oco_allowed: bool):
        self.__oco_allowed = oco_allowed

    def contains_stablecoin(self) -> bool:
        """
        Returns true if the instrument contains a stablecoin

        :return: True or False
        """
        if StableCoin.is_stablecoin(self.__base_asset) or StableCoin.is_stablecoin(self.__quote_asset):
            return True
        return False

    @property
    def stablecoin(self) -> Union[StableCoin, None]:
        """
        Returns the stablecoin enum if this instrument contains a stablecoin, else None

        :return: StableCoin or None
        """
        if self.contains_stablecoin():
            if StableCoin.is_stablecoin(self.__base_asset):
                return StableCoin.str_to_stablecoin(self.__base_asset)
            return StableCoin.str_to_stablecoin(self.__quote_asset)
        return None


# TODO get this out of here and into utils
def filter_instruments(
        instruments: List[Instrument],
        instruments_or_symbols_to_filter: Union[str, Instrument, List[str], List[Instrument]]
) -> List[Instrument]:
    """
    Filters a list of instruments using either a single Instrument or symbol (Instrument.symbol) or a list of
    either of those. Note that if filtering by list of Instruments, then all Instrument properties must be the same
    to be considered equal.

    :param instruments: List of instruments to filter
    :param instruments_or_symbols_to_filter: Either a single instrument or string, or a list of Instrument objects or a list of strings
    :return: The filtered list
    :raises DataClassException: If the type is unknown, if the filter list is empty, general exception if the list could not
        be filtered
    """
    filter_type: Type = type(instruments_or_symbols_to_filter)
    if filter_type != str and filter_type != Instrument and filter_type != list:
        raise DataClassException(f"Unknown type: {filter_type}")
    elif filter_type == str or filter_type == Instrument:
        instruments_or_symbols_to_filter: Union[List[str], List[Instrument]] = [instruments_or_symbols_to_filter]

    if not len(instruments_or_symbols_to_filter):
        raise DataClassException(f"The filter list was empty")

    if type(instruments_or_symbols_to_filter[0]) == Instrument:
        return [
            instrument for instrument in instruments if instrument in instruments_or_symbols_to_filter
        ]

    if type(instruments_or_symbols_to_filter[0]) == str:
        return [
            instrument for instrument in instruments if instrument.symbol in instruments_or_symbols_to_filter
        ]

    raise DataClassException(f"Could not filter instruments, filter: {instruments_or_symbols_to_filter}")
