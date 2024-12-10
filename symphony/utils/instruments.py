from symphony.data_classes import Instrument
from symphony.exceptions import UtilsException
from typing import List, Union, Type


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
    :raises UtilsException: If the type is unknown, if the filter list is empty, general exception if the list could not
        be filtered
    """
    if not isinstance(instruments_or_symbols_to_filter, str) \
            and not isinstance(instruments_or_symbols_to_filter, Instrument) \
            and not isinstance(instruments_or_symbols_to_filter, list):
        raise UtilsException(f"Unknown type: {type(instruments_or_symbols_to_filter)}")
    elif isinstance(instruments_or_symbols_to_filter, Instrument) or isinstance(instruments_or_symbols_to_filter, str):
        instruments_or_symbols_to_filter: Union[List[str], List[Instrument]] = [instruments_or_symbols_to_filter]

    if not len(instruments_or_symbols_to_filter):
        raise UtilsException(f"The filter list was empty")

    if isinstance(instruments_or_symbols_to_filter[0], Instrument):
        return [
            instrument for instrument in instruments if instrument in instruments_or_symbols_to_filter
        ]

    if isinstance(instruments_or_symbols_to_filter[0], str):
        # Handle ccxt style symbols
        instruments_or_symbols_to_filter = [symbol.replace("/", "") for symbol in instruments_or_symbols_to_filter]
        return [
            instrument for instrument in instruments if instrument.symbol in instruments_or_symbols_to_filter
        ]

    raise UtilsException(f"Could not filter instruments, filter: {instruments_or_symbols_to_filter}")


def get_instrument(
        instruments: List[Instrument],
        instrument_or_symbol_to_filter: Union[str, Instrument]
    ) -> Instrument:
    """
    Returns a single instrument from a filter

    :param instruments: List of instruments to filter
    :param instrument_or_symbol_to_filter: Either a single instrument or string, or a list of Instrument objects or a list of strings
    :return: The filtered list
    :raises UtilsException: If the type is unknown, if the filter list is empty, general exception if the list could not
        be filtered
    """
    if not isinstance(instrument_or_symbol_to_filter, str) and not isinstance(instrument_or_symbol_to_filter, Instrument):
        raise UtilsException(f"Wrong type. Type: {instrument_or_symbol_to_filter}")

    filtered: List[Instrument] = filter_instruments(instruments, instrument_or_symbol_to_filter)
    if len(filtered) == 1:
        return filtered[0]
    else:
        raise UtilsException(f"Found more than one result for get_instrument!")


def get_symbol(symbol_or_instrument: Union[str, Instrument]) -> str:
    """
    returns symbol from either a symbol or an instrument

    :param symbol_or_instrument: The symbol or instrument
    :return: Symbol
    :raises UtilsException: If type unknown
    """
    if isinstance(symbol_or_instrument, str):
        if "/" in symbol_or_instrument:
            return symbol_or_instrument.replace("/", "")
        return symbol_or_instrument
    elif isinstance(symbol_or_instrument, Instrument):
        return symbol_or_instrument.symbol
    else:
        raise UtilsException(f"Unknown type: {type(symbol_or_instrument)}")