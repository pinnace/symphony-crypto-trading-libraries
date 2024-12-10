import fastjsonschema
import os
import json
from typing import Callable, List
from .schema_utils import SchemaUtils
from .schema_utils import schema
from .schema_exception import SchemaError


class SchemaKit():
    """Returns base objects for price history, indicators, etc
    
    """
    

    @staticmethod
    @schema(filename='price-history/price_history_schema.json')
    def standard_price_history() -> dict:
        """
        Return a standard price_history object

        Returns:
            dict:price_history
        """
        return {
            "instrument" : "",
            "timescale" : "",
            "price_history" : []
        }

    @staticmethod
    @schema(filename='price-history/definitions/price_history_item.json')
    def standard_price_history_item() -> dict:
        """
        Return a standard price_history_item object

        Returns:
            dict:price_history_item
        """
        
        return {
            "candle" : {
                "timestamp" : None,
                "volume" : None,
                "open" : None,
                "high" : None,
                "low" : None,
                "close" : None
            }
        }

    @staticmethod
    @schema(filename='indicators/indicator_flow_schema.json')
    def standard_flow(instrument: str = "", timeframe: str = "", digits: int = 6, ohlc_price_history: List[list] = [], indicators: List[dict] = []) -> dict:
        """
        Get a standard flow object. Set digits as 6 because this 
        would be "safe", as orders would fail and any risk levels
        would not be rounded to unsafe amounts

        Args:
            instrument (optional, str): The financial instrument
            timeframe (optional, str): The timeframe
            digits (optional, int): The digits. Defaults to a 'safe' 6
            ohlc_price_history (optional, List[list]): OHLC history
            indicators (optional, List[dict]): Optional indicators

        Returns:
            dict:Flow object
        """

        if not isinstance(instrument, str):
            raise SchemaError(__name__ + ": {} is not a string".format(instrument))
        if not isinstance(timeframe, str):
            raise SchemaError(__name__ + ": {} is not a string".format(timeframe))
        if not isinstance(digits, int):
            raise SchemaError(__name__ + ": {} is not an int".format(digits))
        if not isinstance(ohlc_price_history, list):
            raise SchemaError(__name__ + ": {} is not a list".format(ohlc_price_history))
        if not isinstance(indicators, list):
            raise SchemaError(__name__ + ": {} is not a list".format(indicators))

        return {
            "instrument" : instrument,
            "timeframe" : timeframe,
            "digits" : digits,
            "price_history" : ohlc_price_history,
            "indicators" : indicators
        }

    @staticmethod
    @schema(filename='indicators/indicator_flow_indicator_schema.json')
    def standard_indicator_for_flow() -> dict:
        """
        Get a standard indicator object for a flow

        Returns:
            dict: Indicator object
        """

        return {
            "name" : "dummy",
            "settings" : {},
            "data" : {}
        }
    

    
    

        