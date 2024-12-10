import numpy as np
from typing import List
import pprint
from symphony.indicators.indicator_registry import IndicatorRegistry
from symphony.indicators.indicator_exception import IndicatorError



class IndicatorKit():
    """Provides various utility functions for indicators

    """
    @staticmethod
    def price_history_to_numpy_flow(price_history: dict) -> np.array:
        """
        Converts standard price history to a flow numpy array

        Args:
            price_history (dict): Standard price history

        Returns:
            np.array:The converted array

        """
        # TODO: Fix so dimensions include indicator slots
        return np.array(
                [
                    list(map(candle['candle'].__getitem__, ['open', 'high', 'low', 'close', 'volume'])) 
                        for candle in price_history['price_history']
                ]
            , dtype=float)

    @staticmethod
    def price_history_to_arr(price_history: dict) -> List[List[float]]:
        """
        Converts standard price history to an array (simple array of OHLCV). 

        Args:
            price_history (dict): Standard price history

        Returns:
            list[list[float]]:The converted array

        """
        return [list(map(candle['candle'].__getitem__, ['open', 'high', 'low', 'close', 'volume'])) for candle in price_history['price_history']]

    @staticmethod
    def ohlc_from_flow(flow: dict) -> np.array:
        """
        Grab the OHLC points from a flow

        Args:
            flow (dict): A standard flow
        
        Returns:
            np.array: Numpy array of ohlc
        """
        if not isinstance(flow, dict):
            raise IndicatorError(__name__ + ": The provided parameter is not a flow")

        try:
            if len(flow["price_history"][0]) != 5:
                raise IndicatorError(__name__ + ": The provided flow does not have the correct number of dimensions")
        except IndexError:
            raise IndicatorError(__name__ + ": The provided flow does not have a candle")

        ohlc_history = flow["price_history"]

        if len(ohlc_history) == 0:
            raise IndicatorError(__name__ + ": Cannot operate on empty flow price history")

        if not isinstance(ohlc_history, type(np.array)):
            ohlc_history = np.array(ohlc_history)
        
        return ohlc_history
    
    @staticmethod
    def strip_nulls(array: np.array) -> np.array:
        """
        Deletes all null values from an array.

        Args:
            array (np.array): The array to strip nulls from

        Returns
            np.array: The stripped array
        """

        if not isinstance(array, type(np.array)):
            array = np.array(array)

        if array.dtype.char in np.typecodes["AllInteger"]:
            null_indices = np.where(array == 0)[0]
        elif array.dtype.char in np.typecodes["Float"]:
            null_indices = np.where(array == 0.0)[0]
        else:
            raise IndicatorError(__name__ + ": Unrecognized type {}".format(array.dtype.char))
        
        return np.delete(array, null_indices)

    @staticmethod
    def get_points(flow: dict, point: str) -> List[float]:
        """
        Get open, high, low, close or volume of a flow's price history

        Args:
            flow (dict): Flow object
            point (str): Either 'open', 'high', 'low', 'close', 'volume'

        Return:
            list[float]: List of requested points 
        """

        switcher = {
            "open" : 0,
            "high" : 1,
            "low" : 2,
            "close" : 3,
            "volume" : 4
        }

        if switcher.get(point, -1) == -1:
            raise IndicatorError(__name__ + ": Invalid price point {}".format(point))

        ohlc_history = flow["price_history"]

        if not isinstance(ohlc_history, type(np.array)):
            ohlc_history = np.array(ohlc_history)

        return ohlc_history[:, switcher.get(point)]

    @staticmethod
    def get_indicator(flow : dict, name: IndicatorRegistry, channel_names: list = [], settings: dict = {}) -> dict:
        """
        Get the values of an indicator. If channel_names specified,
            return the specified channels

        Args:
            flow (int): The flow
            name (IndicatorRegistry.VALUE): The indicator name from IndicatorRegistry
            channel_names (list, optional): The list of channel names to fetch. If unspecified
                fetch all channel names
            settings (dict, optional): Optionally specifiy a setting to filter on (e.g. if multiple 
                    moving averages are present). Should be in format {"period" : 5}.
        
        Returns:
            (dict): Channels will be returned in a dictionary. If indicator not found, returns empty dictionary.

        Raises:
            IndicatorError: If the name does not exist in IndicatorRegistry
            IndicatorError: If the name is Indicator.PRICE_HISTORY
            IndicatorError: If the channel name does not exist
            IndicatorError: If the indicator with the specified setting does not exist
        """
        if name is IndicatorRegistry.PRICE_HISTORY:
            raise IndicatorError(__name__ + ": Cannot use IndicatorRegistry.PRICE_HISTORY")

        if not name in set(item.value for item in IndicatorRegistry):
            raise IndicatorError(__name__ + ": Unrecognized indicator value {}".format(name))

        for indicator in flow["indicators"]:
            if indicator["name"] == name.name.lower():
                # NOTE: Undocumented behavior? https://stackoverflow.com/questions/9323749/python-check-if-one-dictionary-is-a-subset-of-another-larger-dictionary
                if (settings and settings.items() <= indicator["settings"].items()) or not settings:
                    if channel_names:
                        for channel in channel_names:
                            if channel not in indicator["data"].keys():
                                raise IndicatorError(__name__ + ": Channel name {} not found in {}".format(channel, name.name))
                        return { channel_name: indicator["data"][channel_name] for channel_name in channel_names } 
                    else:
                        return indicator["data"]



        if settings:
            raise IndicatorError(__name__ + ": Could not find indicator {} with settings {}".format(name.name, pprint.pformat(settings)))

        return {}
    
    @staticmethod
    def get_indicator_for_flow(name: str, settings: dict, data: dict) -> dict:
        """
        Generate an indicator obj to append to the ["indicators"] list in flow.
            Does not actually add to the flow

        Args:
            name (str): The name of the indicator
            settings (dict): The settings for the indicator
            data (dict): The channel dict
        
        Return:
            (dict): The indicator onbject
        """
        indicator_obj = {}
        indicator_obj["name"] = name.lower()
        indicator_obj["settings"] = settings
        indicator_obj["data"] = data
        return indicator_obj
    # TODO: Implement way to get only desired slice from flow
    @staticmethod
    def get_true_high(flow: dict, index: int) -> float:
        """
        Get the true high of the specified index. The true high is the greater
            value of the high at `index` or the previous close

        Args:
            flow (dict): The flow
            index (int): The index to find the true high at
        
        Returns:
            (float): The true high
        """
        if not index:
            raise IndicatorError(__name__ + ": Cannot calculate true high for index {}".format(index))

        highs = IndicatorKit.get_points(flow, "high")
        closes = IndicatorKit.get_points(flow, "close")
    
        return max(highs[index], closes[index - 1])
    
    @staticmethod
    def get_true_low(flow: dict, index: int) -> float:
        """
        Get the true low of the specified index. The true low is the lesser
            value of the low at `index` or the previous close

        Args:
            flow (dict): The flow
            index (int): The index to find the true low at
        
        Returns:
            (float): The true low

        Raises:
            IndicatorError: If the index is 0
        """
        if not index:
            raise IndicatorError(__name__ + ": Cannot calculate true low for index {}".format(index))

        lows = IndicatorKit.get_points(flow, "low")
        closes = IndicatorKit.get_points(flow, "close")

        return min(lows[index], closes[index - 1])

    @staticmethod
    def get_true_range_candle(flow: dict, index: int) -> float:
        """
        Get the true range of a candle. True range defined as the largest of the following:
            Difference between this candle's high and low, the difference between this candle's
            high and the previous candle's close, difference between this candle's low and the previous
            candle's low

        Args:
            flow (dict): The flow
            index (int): The index to find the true range of

        Returns:
            (float): The true range
        
        Raises:
            IndicatorError: If the index is 0
        """

        if not index:
            raise IndicatorError(__name__ + ": Cannot calculate true range for index {}".format(index))

        lows = IndicatorKit.get_points(flow, "low")
        highs = IndicatorKit.get_points(flow, "high")
        closes = IndicatorKit.get_points(flow, "close")

        return max(highs[index] - lows[index], abs(highs[index] - closes[index-1]), abs(lows[index] - closes[index - 1]))

    @staticmethod
    def get_true_range(flow: dict, start_index: int, end_index: int) -> float:
        """
        Get the true range of a slice. Difference between highest true high and lowest true low

        Args:
            flow (dict): Flow object
            start_index (int): Starting index of slice to analyse
            end_index (int): Ending index of slice to analyse
        
        Return:
            (float): The true range
        """

        true_high = max( [IndicatorKit.get_true_high(flow,i) for i in range(start_index, end_index + 1)] )
        true_low = min( [IndicatorKit.get_true_low(flow,i) for i in range(start_index, end_index + 1)] )
        return true_high - true_low
        
    @staticmethod
    def pad_to_length(array_to_pad: list, length: int) -> list:
        """
        Pad an array to specified length. Reverses strip_nulls.

        Args:
            array_to_pad (list): The array to pad with zeros
            length (int): The length to pad
        
        Returns:
            (list):The padded array
        """
        if not isinstance(array_to_pad, list):
            raise IndicatorError(__name__ + ": Supplied parameter is not an array")

        default_val = 0 if array_to_pad and isinstance(array_to_pad[0], int) else 0.0
        
        return ([default_val] * (length - len(array_to_pad))) + array_to_pad if len(array_to_pad) < length else array_to_pad

    @staticmethod
    def append_ohlc_into_flow(flow: dict, open_price: float, high_price: float, low_price: float, close_price: float, volume: int) -> dict:
        """
        Insert a set of prices into a flow

        Args:
            flow (dict): Standard dict object
            open_price (float): Open price
            high_price (float): High price
            low_price (float): Low price
            close_price (float): Close price
            volume (flow): Volume
        
        Returns:
            (dict): The flow

        """
        ohlc_bar = [None] * 5
        ohlc_bar[0] = open_price
        ohlc_bar[1] = high_price
        ohlc_bar[2] = low_price
        ohlc_bar[3] = close_price
        ohlc_bar[4] = volume


        flow["price_history"].append(ohlc_bar)

        return flow
    # TODO: Unit test
    @staticmethod
    def insert_indicator_into_flow(flow: dict, indicator: dict) -> None:
        """ 
        Insert an indicator into a flow inplace. Performs some simple dupe checking.

        Args:
            flow (dict): Flow object
            indicator (dict): An indicator object
        
        Returns:
            None
        
        Raises:
            IndicatorError: If indicator already exists
        """

        if flow["indicators"]:
            for current_indicator in flow["indicators"]:
                if current_indicator["name"] == indicator["name"]:
                    raise IndicatorError(__name__ + ": The flow already contains an indicator with name {}".format(indicator["name"]))
        
        flow["indicators"].append(indicator)
        return None
    # TODO: Unit test
    @staticmethod
    def update_indicator_for_flow(flow: dict, indicator: dict) -> None:
        """
        Update the values of an indicator in the flow. Throws error
            if the flow does not have an indicator with the correct name

        Args:
            flow (dict): Flow object
            indicator (dict): An indicator object
        
        Returns:
            None
        
        Raises:
            IndicatorError: If indicator does not exist
        """
            
        if not flow["indicators"] or indicator["name"] not in [ind["name"] for ind in flow["indicators"]]:
            raise IndicatorError(__name__ + ": The indicator with name {} is not present or the flow is empty".format(indicator["name"]))

        for i, ind in enumerate(flow["indicators"]):
            if ind["name"] == indicator["name"]:
                flow["indicators"][i] = indicator
                break
        return None

    @staticmethod
    def indicator_present_in_flow(flow: dict, indicator_name: str) -> bool:
        """
        Returns True if the indicator is in the flow, false otherwise

        Args:
            flow (dict): Standard flow
            indicator_name (str): The indicator name
        
        Returns
            (bool): True if the indicator name is found, false otherwise
        """

        return True if indicator_name in [ind["name"] for ind in flow["indicators"]] else False
        

    
    

