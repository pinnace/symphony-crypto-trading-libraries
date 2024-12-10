import pandas as pd
from symphony.enum import Timeframe
from symphony.exceptions import UtilityClassException
from symphony.data_classes import PriceHistory
from typing import List, Union, Optional, Tuple, Generator
from symphony.enum.timeframe import timeframe_to_numpy_string
import pytz


# TODO: Must do more complicated bar calculations for equities


def round_to_minute(timestamp: pd.Timestamp, minutes: int = 1) -> pd.Timestamp:
    """
    Rounds a timestamp at an arbitrary time by flooring it to the nearest past minute granularity

    :param timestamp: (`pd.Timestamp`) Pandas timestamp
    :param minutes: (`int`) Number of minutes to round to
    :return: (`pd.Timestamp`) The rounded timestamp
    """

    return timestamp.floor(f'{minutes}min')


def round_to_timeframe(timestamp: pd.Timestamp, timeframe: Timeframe) -> pd.Timestamp:
    """
    Rounds a timestamp at an arbitrary time to the nearest Timeframe.TIMEFRAME granularity

    :param timestamp: Pandas timestamp
    :param timeframe: Timeframe
    :return: Timestamp
    :raises UtilityClassException: If the supplied timeframe is not recognized
    """
    if not isinstance(timeframe, Timeframe):
        raise UtilityClassException(f"Not a timeframe: {timeframe}")
    return round_to_minute(timestamp, timeframe.value)


def get_last_complete_bar_time(timeframe: Timeframe) -> pd.Timestamp:
    """
    Get the timestamp of the most recently completed bar for a given timeframe

    :param timeframe: (`Timeframe.TIMEFRAME`) The timeframe
    :return: (`pd.Timestamp`) Timestamp of the bar
    """

    return pd.Timestamp.now(tz='UTC').floor(f"{timeframe.value}min") - pd.Timedelta(minutes=timeframe.value)


def get_current_bar_open_time(timeframe: Timeframe) -> pd.Timestamp:
    """
    Get the start time of the 'now' bar

    :param timeframe: (`Timeframe.TIMEFRAME`) The timeframe
    :return: (`pd.Timestamp`) Timestamp of the bar
    """

    return pd.Timestamp.now(tz='UTC').floor(f"{timeframe.value}min")


def to_unix_time(timestamp: Union[pd.Timestamp, int], resolution: str = 's') -> int:
    """
    Convert pandas timestamp to unix time at given resolution

    :param timestamp: (`pd.Timestamp`, `int`) Pandas timestamp or UNIX timestamp
    :param resolution: 's', 'm', 'M', 'H'
    :return: UNIX timestamp
    :raises UtilityClassException: If the resolution is not implemented
    """
    timestamp_value = timestamp
    if isinstance(timestamp, pd.Timestamp):
        timestamp_value = timestamp.value

    diff = (timestamp - pd.Timestamp("1970-01-01", tz='UTC'))
    if resolution == 's':
        return diff // pd.Timedelta('1s')
    if resolution == 'ms':
        return diff // pd.Timedelta('1ms')
    else:
        raise UtilityClassException(f"Resolution \'{resolution}\' not implemented")


def get_timestamp_of_num_bars_back(timeframe: Timeframe, timestamp: pd.Timestamp, num_bars: int) -> pd.Timestamp:
    """
    Calculate the time of the bar `num_bars` back from `timestamp` at resolution `timeframe`

    :param timeframe: Standard timeframe
    :param timestamp: Start time (most recent time)
    :param num_bars: num bars back
    :return: Past timestamp
    """
    rounded_timestamp = round_to_minute(timestamp, timeframe.value)
    minutes = timeframe.value * (num_bars - 1)
    return rounded_timestamp - pd.Timedelta(minutes=minutes)


def get_timestamp_of_num_bars_forward(timeframe: Timeframe, timestamp: pd.Timestamp, num_bars: int) -> pd.Timestamp:
    """
    Calculate the time of the bar `num_bars` forward from `timestamp` at resolution `timeframe`

    :param timeframe: Standard timeframe
    :param timestamp: Start time (most recent time)
    :param num_bars: num bars forward
    :return: Future timestamp
    """
    rounded_timestamp = round_to_minute(timestamp, timeframe.value)
    minutes = timeframe.value * num_bars
    return rounded_timestamp + pd.Timedelta(minutes=minutes)


def get_num_bars_timestamp_to_present(timestamp: pd.Timestamp, timeframe: Timeframe, incomplete_bar: Optional[bool] = False) -> int:
    """
    Get the number of bars between a start time and the current time, rounding to Timeframe

    :param timestamp: Start timestamp
    :param timeframe: Applicable timeframe
    :param incomplete_bar: Use incomplete bar (i.e. current open bar), defaults to false
    :return: Number of bars
    """
    if incomplete_bar:
        curr_bar = get_current_bar_open_time(timeframe)
    else:
        curr_bar = get_last_complete_bar_time(timeframe)
    if timestamp > curr_bar:
        raise UtilityClassException(f"{timestamp} is more recent than curr bar open {curr_bar}")
    numpy_timeframe = timeframe_to_numpy_string(timeframe)
    return abs(
        pd.Timedelta((curr_bar + pd.Timedelta(numpy_timeframe)) - timestamp) // pd.Timedelta(numpy_timeframe)
    )


def get_num_bars_timestamp_to_timestamp(start_timestamp: pd.Timestamp, end_timestamp: pd.Timestamp, timeframe: Timeframe) -> int:
    """
    Get the number of bars between a start time and end time, rounding to Timeframe

    :param start_timestamp: Start timestamp
    :param end_timestamp: End timestamp
    :param timeframe: Applicable timeframe
    :return: Number of bars
    """
    if start_timestamp > end_timestamp:
        raise UtilityClassException(f"{start_timestamp} is more recent than curr bar open {end_timestamp}")
    return abs(
        pd.Timedelta(end_timestamp - start_timestamp) // pd.Timedelta(timeframe_to_numpy_string(timeframe))
    )

def filter_start(indices: List[pd.Timestamp], start_ts: pd.Timestamp) -> List[pd.Timestamp]:
    """
    Selects timestamps after an arbitrary date

    :param indices: List to filter
    :param start_ts: starting timestamp
    :return:
    """

    return [index for index in indices if index >= start_ts]


def chunk_times(
        start: pd.Timestamp,
        timeframe: Timeframe,
        interval_in_minutes: int,
        end: Optional[pd.Timestamp] = None,
        until_now: Optional[bool] = False,
        end_inclusive: Optional[bool] = True
) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """
    For breaking up large timespans into smaller chunks (e.g. for API calls)

    :param start: The starting time
    :param timeframe: Timeframe we are operating on
    :param interval_in_minutes: How much time each chunk should span
    :param end: Optional end time. If not supplied, will use last completed bar.
    :param until_now: Optionally use current bar instead of last completed bar
    :param end_inclusive: If the end date is inclusive
    :return: A list of tuples [(chunk_start, chunk_end), ...]
    :raises UtilityClassException: If `end` and `until_now` are specified together (which they should not be)
    """

    if end and until_now:
        raise UtilityClassException("`end` and `until_now` should not be specified together")

    def to_utc(timestamp: pd.Timestamp) -> pd.Timestamp:
        if not timestamp.tz:
            return timestamp.tz_localize('UTC')
        else:
            if timestamp.tz != pytz.UTC:
                return timestamp.tz_convert('UTC')
        return timestamp

    start = to_utc(start)
    start = round_to_timeframe(start, timeframe)

    if not end:
        if not until_now:
            end_time = get_last_complete_bar_time(timeframe)
        else:
            end_time = get_current_bar_open_time(timeframe)
    else:
        end_time = end
    end_time = to_utc(end_time)

    chunks: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    chunk_start = start
    while chunk_start < end_time:
        chunk_end: pd.Timestamp = chunk_start + pd.Timedelta(minutes=interval_in_minutes)
        if chunk_end > end_time:
            chunk_end = end_time
        chunk_end = round_to_timeframe(chunk_end, timeframe)
        one_bar_past_end: pd.Timestamp = get_timestamp_of_num_bars_forward(timeframe, chunk_end, 1)
        if end_inclusive:
            chunk = (chunk_start, chunk_end)
        else:
            chunk = (chunk_start, one_bar_past_end)

        chunks.append(chunk)

        chunk_start = one_bar_past_end

    return chunks


def standardize_index(price_history: PriceHistory, index: Union[int, pd.Timestamp]) -> int:
    """
    Pass in an index, either integer or datetime, and get normalized integer index in response
    :param price_history: This price history
    :param index: The index
    :return: The integer index
    """
    df = price_history.price_history
    if type(index) == int:
        if index < 0:
            index: int = len(df) + index

        else:
            index: int = index
    elif type(index) == pd.Timestamp:
        index: int = df.index.get_loc(index)
    else:
        raise UtilityClassException(f"Type {type(index)} of pattern index is not recognised")
    return index
