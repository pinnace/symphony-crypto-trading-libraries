
import unittest
import json
import os
import sys
import logging
from pprint import pprint
from os.path import join, dirname
import fastjsonschema
import numpy as np
from symphony.schema.schema_kit import SchemaKit
from symphony.indicators.demarkcountdown.td_countdown import td_buy_countdown
from symphony.indicators.indicators import Indicators
from symphony.indicators.indicator_kit import IndicatorKit
from symphony.indicators.indicator_registry import IndicatorRegistry
from symphony.tests.suite_utils import TestingUtils

class DemarkCountdownTest(unittest.TestCase):
    flow: dict = TestingUtils.dummy_flow()
    price_history: dict = TestingUtils.dummy_price_history()

    def test_td_buy_countdown(self):
        flow = self.flow
        td_buy_countdown(flow)


        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")
    
    def test_td_countdown(self):
        flow = self.flow
        price_history = self.price_history
        
        flow["indicators"].append(
            IndicatorKit.get_indicator_for_flow(
                IndicatorRegistry.TD_COUNTDOWN.name, 
                {}, 
                Indicators.td_countdown(flow)
                )
        )
        countdowns = IndicatorKit.get_indicator(flow, IndicatorRegistry.TD_COUNTDOWN, channel_names=["buy_countdowns", "sell_countdowns"])
        buy_countdown_indices = np.where(np.array(countdowns["buy_countdowns"]) == 1)[0]
        sell_countdown_indices = np.where(np.array(countdowns["sell_countdowns"]) == 1)[0]

        # Make sure all calculated countdowns are present
        for buy_countdown_index in buy_countdown_indices:
            self.assertEqual(price_history["price_history"][buy_countdown_index]["comment"]["countdown_index"], 13)
            self.assertEqual(price_history["price_history"][buy_countdown_index]["comment"]["countdown_type"], 'BUY')
        
        for sell_countdown_index in sell_countdown_indices:
            self.assertEqual(price_history["price_history"][sell_countdown_index]["comment"]["countdown_index"], 13)
            self.assertEqual(price_history["price_history"][sell_countdown_index]["comment"]["countdown_type"], 'SELL')
        
        # Make sure all 
        # find index of first setups. Setups early in test price history might not be calculable
        first_buy_setup = 0
        first_sell_setup = 0
        for index, bar in enumerate(price_history["price_history"]):
            if index > 15: # Account for price flip + setup, min index for first detectable setup
                if not first_buy_setup and bar["comment"]["setup_index"] == 9 and bar["comment"]["setup_type"] == 'BUY':
                    first_buy_setup = index
                if not first_sell_setup and bar["comment"]["setup_index"] == 9 and bar["comment"]["setup_type"] == 'SELL':
                    first_sell_setup = index
                if first_buy_setup and first_sell_setup:
                    break

        for i in range(first_buy_setup, len(price_history["price_history"])):
            if price_history["price_history"][i]["comment"]["countdown_index"] == 13:
                if price_history["price_history"][i]["comment"]["countdown_type"] == 'BUY':
                    self.assertEqual(countdowns["buy_countdowns"][i], 1)
                if price_history["price_history"][i]["comment"]["countdown_type"] == 'SELL':
                    self.assertEqual(countdowns["sell_countdowns"][i], 1)
            
        

        print(__name__ + "." + sys._getframe(  ).f_code.co_name + ": Unit test passed")

if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "DemarkCountdownTest.test_td_buy_countdown" ).setLevel( logging.DEBUG )
    logging.getLogger( "DemarkCountdownTest.test_td_countdown" ).setLevel( logging.DEBUG )
    unittest.main()