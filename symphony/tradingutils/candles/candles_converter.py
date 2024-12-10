import json
from pprint import pprint
import os
from datetime import datetime
from .candle_exception import CandleError
from .candle_parser import PriceHistoryParser
from symphony.schema import SchemaKit
from pandas import DataFrame


class CandlesConverter():
    """Convert various candle formats to a standard format

    To use the API, the raw data obtained from provider must first open
    the data from a file, then call the parse() method which will operate on the raw data.

    """

    def __init__(self):
        self.__raw_data = None
        self.parser = PriceHistoryParser()

    def price_history_from_file(self, filename: str) -> None:
        """
        Get the raw price history from a file. Currently only a json handler defined.

        Args:
            filename (str): The location of the raw price history

        Returns:
            None:If no error thrown, then the raw price history was successfully parsed

        Raises:
            CandleError: If no handler for the file extension defined, or if the file
                could not be located
        """
        if os.path.exists(filename):
            _, file_extension = os.path.splitext(filename)
            if file_extension == ".json":
                with open(filename, "r") as f:
                    self.__raw_dict = json.loads(f.read())
            else:
                raise CandleError(__name__ + ": No handler for {} extension defined".format(file_extension))
        else:
            raise CandleError(__name__ + ": Could not locate file with name: " + str(filename))
        return
    
    @staticmethod
    def price_history_to_csv(price_history: dict, file_path: str, datetime_format: str = None) -> None:
        """
        Convert the price history and dump to a csv file

        Args:
            price_history (dict): The price history object
            file_path (str): The file to dump to
            datetime_format (str, optional): Optional datetime format to convert to e.g. "%Y-%m-%d %H:%M:%S"

        Retuns:
            None
        """
        vals_in_ph = lambda price_history, data_point: [candle["candle"][data_point] for candle in price_history["price_history"]]
        pd_obj = {}
        for key in price_history["price_history"][0]["candle"].keys():
            pd_obj[key] = vals_in_ph(price_history, key)

        if datetime_format:
            pd_obj["datetime"] = map(lambda x: datetime.fromtimestamp(x).strftime(datetime_format), pd_obj["timestamp"])
            
        df = DataFrame(data=pd_obj)
        df.to_csv(file_path, index=False)




    def parse(self, parser_type: str) -> dict:
        """
        Calls internal CandleParser object to parse the raw data

        Args:
            parser_type (str): The type of parser to use (e.g. 'oanda')

        Returns:
            dict:A standard price history object
        
        Raises:
            CandleError: If there was no raw data loaded.
        """
        if self.__raw_dict == None:
            raise CandleError(__name__ + ": No raw data was loaded")
        return self.parser.parse(self.__raw_dict, parser_type)

    

    

