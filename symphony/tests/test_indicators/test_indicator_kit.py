import unittest
import json
import os
import sys
import logging
from pprint import pprint
from symphony.tradingutils.candles.candles_converter import CandlesConverter
from symphony.indicators.indicator_kit import IndicatorKit
from symphony.indicators.indicators import Indicators
from symphony.indicators.indicator_exception import IndicatorError
from symphony.tests.suite_utils import TestingUtils
from symphony.indicators.indicator_registry import IndicatorRegistry
from symphony.schema.schema_kit import SchemaKit


class IndicatorKitTest(unittest.TestCase):
    price_history = TestingUtils.dummy_price_history()
    def test_array_to_numpy(self):
        price_history = self.price_history

        nph = IndicatorKit.price_history_to_numpy_flow(price_history)
        
        for index, bar in enumerate(nph):
            self.assertEquals(bar[0], price_history["price_history"][index]["candle"]["open"])
            self.assertEquals(bar[1], price_history["price_history"][index]["candle"]["high"])
            self.assertEquals(bar[2], price_history["price_history"][index]["candle"]["low"])
            self.assertEquals(bar[3], price_history["price_history"][index]["candle"]["close"])
            self.assertEquals(bar[4], price_history["price_history"][index]["candle"]["volume"])
        
        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")

    def test_price_history_to_flow(self):
        price_history = self.price_history

        flow_history = SchemaKit.standard_flow()
        flow_history["price_history"] = IndicatorKit.price_history_to_arr(price_history)
        for index, bar in enumerate(flow_history["price_history"]):
            self.assertEquals(bar[0], price_history["price_history"][index]["candle"]["open"])
            self.assertEquals(bar[1], price_history["price_history"][index]["candle"]["high"])
            self.assertEquals(bar[2], price_history["price_history"][index]["candle"]["low"])
            self.assertEquals(bar[3], price_history["price_history"][index]["candle"]["close"])
            self.assertEquals(bar[4], price_history["price_history"][index]["candle"]["volume"])

        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")

    def test_ohlc_from_flow(self):
        empty_price_history = SchemaKit.standard_price_history()
        
        empty_flow = SchemaKit.standard_flow()
        empty_flow["price_history"] = IndicatorKit.price_history_to_arr(empty_price_history)

        # Empty flow should raise an error
        with self.assertRaises(IndicatorError):
            IndicatorKit.ohlc_from_flow(empty_flow)

        price_history = self.price_history

        # Trying to supply price history instead of flow should raise error
        with self.assertRaises(IndicatorError):
            IndicatorKit.ohlc_from_flow(price_history)

        flow_history = SchemaKit.standard_flow()
        flow_history["price_history"] = IndicatorKit.price_history_to_arr(price_history)

        ohlc_history = IndicatorKit.ohlc_from_flow(flow_history)

        for i, bar in enumerate(price_history["price_history"]):
            self.assertEqual(ohlc_history[i,0], bar["candle"]["open"])
            self.assertEqual(ohlc_history[i,1], bar["candle"]["high"])
            self.assertEqual(ohlc_history[i,2], bar["candle"]["low"])
            self.assertEqual(ohlc_history[i,3], bar["candle"]["close"])
            self.assertEqual(ohlc_history[i,4], bar["candle"]["volume"])


        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")

    def test_get_points(self):
        price_history = self.price_history
   
        flow_history = SchemaKit.standard_flow()
        flow_history["price_history"] = IndicatorKit.price_history_to_arr(price_history)

        with self.assertRaises(IndicatorError):
            nx = IndicatorKit.get_points(flow_history, "nonexistent")

        opens = IndicatorKit.get_points(flow_history, "open")
        highs = IndicatorKit.get_points(flow_history, "high")
        lows = IndicatorKit.get_points(flow_history, "low")
        closes = IndicatorKit.get_points(flow_history, "close")
        volumes = IndicatorKit.get_points(flow_history, "volume")

        for i, o in enumerate(price_history["price_history"]):
            self.assertEqual(opens[i], o["candle"]["open"])
        for i, h in enumerate(price_history["price_history"]):
            self.assertEqual(highs[i], h["candle"]["high"])
        for i, l in enumerate(price_history["price_history"]):
            self.assertEqual(lows[i], l["candle"]["low"])
        for i, c in enumerate(price_history["price_history"]):
            self.assertEqual(closes[i], c["candle"]["close"])
        for i, v in enumerate(price_history["price_history"]):
            self.assertEqual(volumes[i], v["candle"]["volume"])


        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
    
    def test_get_indicator(self):
        price_history = self.price_history
   
        flow_history = SchemaKit.standard_flow()
        flow_history["price_history"] = IndicatorKit.price_history_to_arr(price_history)

        indicator_obj = SchemaKit.standard_indicator_for_flow()


        indicator_obj = IndicatorKit.get_indicator_for_flow(IndicatorRegistry.BEARISH_PRICE_FLIP.name, {}, Indicators.bearish_price_flip(flow_history))
        flow_history["indicators"].append(indicator_obj)


        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")

if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "IndicatorKitTest.test_array_to_numpy" ).setLevel( logging.DEBUG )
    logging.getLogger( "IndicatorKitTest.test_price_history_to_flow" ).setLevel( logging.DEBUG )
    logging.getLogger( "IndicatorKitTest.test_ohlc_from_flow" ).setLevel( logging.DEBUG )
    logging.getLogger( "IndicatorKitTest.test_get_indicator" ).setLevel( logging.DEBUG )
    unittest.main()