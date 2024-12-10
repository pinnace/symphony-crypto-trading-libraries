
import os
import logging
import sys
from typing import Callable
from symphony.tradingutils.candles.candles_converter import CandlesConverter
from symphony.schema.schema_utils import SchemaUtils
from symphony.schema.schema_kit import SchemaKit
from symphony.indicators.indicator_kit import IndicatorKit

class TestingUtils():

    @staticmethod
    def dummy_price_history() -> dict:
        """
        Return some testing data.

        Returns:
            dict:price_history

        Raises:
            AssertionError: If the close price does not match as expected.
        """
        
        data_dir = os.path.dirname(os.path.realpath(__file__)) + "/data/"
        converter = CandlesConverter()

        
        converter.price_history_from_file(data_dir + "test-price-history-raw-merged.json")
        
        price_history = converter.parse("merged_trading_view")
        
        assert(price_history["price_history"][-1]["candle"]["close"] == 1.10181)
        SchemaUtils.validate_price_history(price_history)

        return price_history
    
    @staticmethod
    def dummy_flow() -> dict:
        """
        Return a dummy flow

        Returns:
            dict:flow

        """

        price_history = TestingUtils.dummy_price_history()
        flow = SchemaKit.standard_flow()
        flow["instrument"] = price_history["instrument"]
        flow["timescale"] = price_history["timescale"]
        flow["digits"] = price_history["digits"]
        flow["price_history"] = IndicatorKit.price_history_to_arr(price_history)

        return flow

