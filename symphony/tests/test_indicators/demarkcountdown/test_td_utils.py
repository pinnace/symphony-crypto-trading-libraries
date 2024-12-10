import unittest
import json
import os
import sys
import logging
from pprint import pprint
from os.path import join, dirname
import fastjsonschema
import numpy as np
from symphony.indicators import Indicators, IndicatorKit, IndicatorRegistry
from symphony.tests.suite_utils import TestingUtils
from symphony.indicators.demarkcountdown.td_utils import get_td_countdown_pattern_start, get_td_countdown_stoploss

class DemarkUtilsTest(unittest.TestCase):
    flow: dict = TestingUtils.dummy_flow()
    price_history: dict = TestingUtils.dummy_price_history()

    def test_get_td_countdown_pattern_start(self):
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

        for buy_countdown_index in buy_countdown_indices:
            start_index = get_td_countdown_pattern_start(flow, buy_countdown_index)
            self.assertEquals(price_history["price_history"][start_index + 6]["comment"]["setup_type"], "BUY")
            self.assertEquals(price_history["price_history"][start_index + 6]["comment"]["setup_index"], 1)
            
        for sell_countdown_index in sell_countdown_indices:
            start_index = get_td_countdown_pattern_start(flow, sell_countdown_index)
            self.assertEquals(price_history["price_history"][start_index + 6]["comment"]["setup_type"], "SELL")
            self.assertEquals(price_history["price_history"][start_index + 6]["comment"]["setup_index"], 1)

    def test_get_td_countdown_stoploss(self):
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
        print(flow["digits"])
        for buy_countdown_index in buy_countdown_indices:
            stop_loss = get_td_countdown_stoploss(flow, buy_countdown_index)
            # lowest low at index 140
            print(stop_loss)
        for sell_countdown_index in sell_countdown_indices:
            stop_loss = get_td_countdown_stoploss(flow, sell_countdown_index)
            print(stop_loss)


if __name__ == "__main__":
    logging.basicConfig( stream=sys.stdout )
    logging.getLogger( "DemarkUtilsTest.test_get_td_countdown_pattern_start" ).setLevel( logging.DEBUG )
    logging.getLogger( "DemarkUtilsTest.test_get_td_countdown_stoploss" ).setLevel( logging.DEBUG )

    unittest.main()