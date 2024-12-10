from symphony.data_classes import PriceHistory
from symphony.enum import timeframe_to_string


def get_log_header(price_history: PriceHistory) -> str:
    """
    Formats a header with symbol, timeframe, and exchange info for use in logging functions

    :param price_history: (`PriceHistory`) Standard price history
    :return: (`str`) [SYMBOL][TIMEFRAME][EXCHANGE]
    """

    symbol: str = price_history.instrument.symbol
    timeframe: str = timeframe_to_string(price_history.timeframe)
    if price_history.instrument.exchange:
        exchange = price_history.instrument.exchange.value
    else:
        exchange = "NO_X"

    return f"[{symbol}][{timeframe}][{exchange}]"


def glh(price_history: PriceHistory) -> str:
    """
    Shorthand for get_log_header

    :param price_history: (`PriceHistory`) Standard price history
    :return: (`str`) [SYMBOL][TIMEFRAME][EXCHANGE]
    """
    return get_log_header(price_history)
