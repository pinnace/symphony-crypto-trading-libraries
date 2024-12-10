
from .candle_exception import CandleError
from symphony.schema import SchemaKit
from symphony.schema.schema_utils import schema


class PriceHistoryParser():
    """Parses various raw formats and converts to standard object
    
    """ 


    @staticmethod
    def parse(raw_price_history: dict, parser_type: str) -> dict:
        """
        Dynamically call parser type, e.g. 'oanda', 'merged_trading_view'

        Args:
            raw_price_history (dict): Raw price history in whatever format it came in
            parser_type (str): Which type of parser to use

        Returns:
            dict:Standard Price History dict

        Raises:
            CandleError: If parser cannot be found
        """
        if parser_type in [parser.replace("_PriceHistoryParser__parse_","") for parser in dir(PriceHistoryParser) if parser.startswith("_PriceHistoryParser__parse")]:
            return getattr(PriceHistoryParser,"_PriceHistoryParser__parse_" + parser_type)(raw_price_history)
        else:
            raise CandleError(__name__ + ": Could not find parser: {}".format(parser_type))

    @staticmethod
    def __parse_oanda(raw_price_history: dict) -> dict:
        """
        Convert object from Oanda style candles object into standard object
        """
        """
        standard_candles = self._standard_candles_dict()
        standard_candles["price_history"] = [None] * len(candles["price_history"])
        for index,candle in enumerate(candles["candles"]):
            standard_candles["price_history"][index] = {}
            standard_candles["price_history"][index]["open"] = float(candle["mid"]["o"])
            standard_candles["price_history"][index]["close"] = float(candle["mid"]["c"])
            standard_candles["price_history"][index]["high"] = float(candle["mid"]["h"])
            standard_candles["price_history"][index]["low"] = float(candle["mid"]["l"])
        # Reverse
        standard_candles["price_history"] = standard_candles["price_history"][::-1]
        return standard_candles
        """

    @staticmethod
    @schema(filename='price-history/price_history_schema.json')
    def __parse_merged_trading_view(raw_price_history: dict) -> dict:
        """
        Convert the cleaned and merged candles from tradingview conversion script.

        Args:
            raw_price_history (dict): The raw price history in trading view merged format (from this repo's root /scripts directory)

        Returns:
            dict:Standard price history object
        """
        
        ph = SchemaKit.standard_price_history()
        ph["instrument"] = raw_price_history["instrument"]
        ph["timescale"] = raw_price_history["timescale"]
        ph["digits"] = raw_price_history["digits"]
        
        for bar in raw_price_history["price_history"]:
            new_bar = SchemaKit.standard_price_history_item()
            new_bar["candle"]["timestamp"] = bar["v"][0]
            new_bar["candle"]["open"] = bar["v"][1]
            new_bar["candle"]["high"] = bar["v"][2]
            new_bar["candle"]["low"] = bar["v"][3]
            new_bar["candle"]["close"] = bar["v"][4]
            new_bar["candle"]["volume"] = bar["v"][5]
            new_bar["comment"] = bar["cd"]
            ph["price_history"].append(new_bar)
        return ph
    
    