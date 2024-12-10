from enum import Enum, auto
from typing import List
from binance.client import Client
from symphony.exceptions import DataClassException

"""
Classes and methods for dealing with timeframes. Contains enum and converters for different
exchange clients
"""


class Timeframe(Enum):
    """
    Class containing supported timeframes
    """

    M1 = 1
    M5 = 5
    M15 = 15
    M30 = 30
    H1 = 60
    H4 = 240
    D1 = 1440

    @staticmethod
    def list() -> List:
        return [timeframe for timeframe in Timeframe]


def integer_to_timeframe(integer: int) -> Timeframe:
    """
    Gets a timefrome from an integer

    :param integer: Integer
    :return: Timeframe
    """
    if integer == 1:
        return Timeframe.M1
    elif integer == 5:
        return Timeframe.M5
    elif integer == 15:
        return Timeframe.M15
    elif integer == 30:
        return Timeframe.M30
    elif integer == 60:
        return Timeframe.H1
    elif integer == 240:
        return Timeframe.H4
    elif integer == 1440:
        return Timeframe.D1
    else:
        raise DataClassException(f"Could not convert {integer} to timeframe")

def get_binance_client_timeframe(timeframe: Timeframe) -> str:
    """
    Converts from standard timeframe to binance.client timeframe

    :param timeframe: Standard timeframe object
    :return: The client timeframe
    """
    if timeframe == Timeframe.M1:
        return Client.KLINE_INTERVAL_1MINUTE
    elif timeframe == Timeframe.M5:
        return Client.KLINE_INTERVAL_5MINUTE
    elif timeframe == Timeframe.M15:
        return Client.KLINE_INTERVAL_15MINUTE
    elif timeframe == Timeframe.M30:
        return Client.KLINE_INTERVAL_30MINUTE
    elif timeframe == Timeframe.H1:
        return Client.KLINE_INTERVAL_1HOUR
    elif timeframe == Timeframe.H4:
        return Client.KLINE_INTERVAL_4HOUR
    elif timeframe == Timeframe.D1:
        return Client.KLINE_INTERVAL_1DAY
    else:
        raise DataClassException(f"Could not identify {timeframe} as valid Binance timeframe")


def timeframe_to_string(timeframe: Timeframe) -> str:
    """
    Converts a timeframe to a string representation

    :param timeframe: (`TimeFrame`) Standard timeframe
    :return: (`str`)
    """

    if timeframe == Timeframe.M1:
        return "M1"
    elif timeframe == Timeframe.M5:
        return "M5"
    elif timeframe == Timeframe.M15:
        return "M15"
    elif timeframe == Timeframe.M30:
        return "M30"
    elif timeframe == Timeframe.H1:
        return "H1"
    elif timeframe == Timeframe.H4:
        return "H4"
    elif timeframe == Timeframe.D1:
        return "D1"
    else:
        raise DataClassException(f"Could not identify {timeframe} ")


def string_to_timeframe(timeframe_str: str) -> Timeframe:
    """
    Returns a standard timeframe from a given string

    :param timeframe_str: String to parse
    :return: Timeframe
    :raises DataClassException: If the timeframe is unknown
    """
    if timeframe_str in ["1m"]:
        return Timeframe.M1
    elif timeframe_str in ["5m"]:
        return Timeframe.M5
    elif timeframe_str in ["15m"]:
        return Timeframe.M15
    elif timeframe_str in ["30m"]:
        return Timeframe.M30
    elif timeframe_str in ["1h"]:
        return Timeframe.H1
    elif timeframe_str in ["4h"]:
        return Timeframe.H4
    elif timeframe_str in ["1d"]:
        return Timeframe.D1
    else:
        raise DataClassException(f"Could not identify timeframe for {timeframe_str}")


def timeframe_to_numpy_string(timeframe: Timeframe) -> str:
    """
    Converts a timeframe to a string that can be used with numpy or pandas freq functions

    :param timeframe: Timeframe to process
    :return: Stringified timeframe
    :raises DataClassException: If the timeframe is unknown
    """
    if timeframe == Timeframe.M1:
        return "1m"
    elif timeframe == Timeframe.M5:
        return "5m"
    elif timeframe == Timeframe.M15:
        return "15m"
    elif timeframe == Timeframe.M30:
        return "30m"
    elif timeframe == Timeframe.H1:
        return "1h"
    elif timeframe == Timeframe.H4:
        return "4h"
    elif timeframe == Timeframe.D1:
        return "1D"
    else:
        raise DataClassException(f"Could not identify timeframe for {timeframe}")