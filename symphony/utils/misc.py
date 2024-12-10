from typing import Union, List, Iterable, Any, Generator
import itertools
from symphony.enum import Timeframe
from symphony.data_classes import Instrument
from symphony.exceptions import UtilsException


def cartesian_product(
        instruments: Union[Instrument, List[Instrument]],
        timeframes: Union[Timeframe, List[Timeframe]]
) -> List[tuple]:
    """
        Return Cartesian Product (combinations) of instruments and timeframes
        as list of tuples

        :param instruments: Either a single or list of instruments
        :param timeframes: Either a single or list of timeframes
        :return: list of tuples (Instrument, Timeframe)
        """

    if type(instruments) != list:
        if type(instruments) != Instrument:
            raise UtilsException(f"Instrument supplied is not a list of "
                                       f"instruments or a single instrument: {instruments}")
        instruments = [instruments]

    if type(timeframes) != list:
        if type(timeframes) != Timeframe:
            raise UtilsException(f"Timeframe supplied is not a list of "
                                       f"timeframes or a single timeframe: {instruments}")
        timeframes = [timeframes]

    return list(
        itertools.product(instruments, timeframes)
    )


def grouper(iterable: Iterable, n: int, fillvalue: Any = None) -> List[Iterable]:
    """
    Returns n-sized chunks of some iterable

    :param iterable: Some iterable
    :param n: Chunk size
    :param fillvalue: Optional fill value, defaults to None
    :return: List of iterables
    """
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def chunker(iterable: Iterable, chunk_size: int) -> List[List]:
    """
    Transforms a list into a generator of a list of lists of chunk_size

    :param iterable: Some iterable
    :param chunk_size: Chunk size
    :return: Generator of lists
    """
    return (iterable[pos:pos + chunk_size] for pos in range(0, len(iterable), chunk_size))
