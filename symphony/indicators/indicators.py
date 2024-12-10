from enum import IntEnum, unique, auto
from typing import List

# Indicator imports
from symphony.indicators.demarkcountdown.td_price_flip import bullish_price_flip, bearish_price_flip
from symphony.indicators.demarkcountdown.td_setup import td_buy_setup, td_sell_setup
from symphony.indicators.demarkcountdown.td_countdown import td_buy_countdown, td_sell_countdown, td_countdown
from symphony.indicators.movingaverage.sma import sma
from symphony.indicators.movingaverage.ema import ema
from symphony.indicators.movingaverage.wma import wma
from symphony.indicators.zigzag.zigzag import zigzag


# End indicator imports

class Indicators():
    #TODO: add docs
    @staticmethod
    def bullish_price_flip(flow: dict, *args, **kwargs) -> dict:
        return bullish_price_flip(flow, *args, **kwargs)

    @staticmethod
    def bearish_price_flip(flow: dict, *args, **kwargs) -> dict:
        return bearish_price_flip(flow, *args, **kwargs)

    @staticmethod
    def td_buy_setup(flow: dict, *args, **kwargs) -> dict:
        return td_buy_setup(flow, *args, **kwargs)

    @staticmethod
    def td_sell_setup(flow: dict, *args, **kwargs) -> dict:
        return td_sell_setup(flow, *args, **kwargs)

    @staticmethod
    def td_buy_countdown(flow: dict, *args, **kwargs) -> dict:
        """
        Calculates the buy countdown and returns all buy countdown
            components as indicator object (price flips, setups, perfect setups, TDST)

        Args:
            flow (dict): Flow object
            setups (dict, optional): Optionally supply the setups object returned by td_setup
            cancellation_qualifier_I (bool, optional): CQI. Defaults to false
            cancellation_qualifier_II (bool, optional): CQII. Defaults to false
            **kwargs (dict, optional): Any settings to pass to setup or price flip
        
        Returns:
            (dict): Indicator object with all buy channels
        """
        return td_buy_countdown(flow, *args, **kwargs)

    @staticmethod
    def td_sell_countdown(flow: dict, *args, **kwargs) -> dict:
        """
        Calculates the sell countdown and returns all sell countdown
            components as indicator object (price flips, setups, perfect setups, TDST)

        Args:
            flow (dict): Flow object
            cancellation_qualifier_I (bool, optional): CQI. Defaults to false
            cancellation_qualifier_II (bool, optional): CQII. Defaults to false
            **kwargs (dict): Any settings to pass to setup or price flip
        
        Returns:
            (dict): Indicator object with all sell channels
        """
        return td_sell_countdown(flow, *args, **kwargs)

    @staticmethod
    def td_countdown(flow: dict, *args, **kwargs) -> dict:
        """
        Calculates both the buy and sell countdowns and returns as a unified
            indicator object

        Args:
            flow (dict): Flow object
            **kwargs (dict): Any settings to pass to setup or price flip
        
        Returns:
            (dict): Indicator object with all channels
        """
        return td_countdown(flow, *args, **kwargs)
    
    @staticmethod
    def sma(flow: dict, *args, **kwargs) -> dict:
        """
        Simple moving average

        Args:
            flow (dict): Flow object
            period (int, optional): The SMA period. Defaults to 5

        Returns:
            (dict): SMA
        """
        return sma(flow, *args, **kwargs)

    @staticmethod
    def ema(flow: dict, *args, **kwargs) -> dict:
        """
        Exponential moving average

        Args:
            flow (dict): Flow object
            period (int, optional): The EMA period. Defaults to 5

        Returns:
            (dict): EMA
        """
        return ema(flow, *args, **kwargs)
    
    @staticmethod
    def wma(flow: dict, *args, **kwargs) -> dict:
        """
        Weighted moving average

        Args:
            flow (dict): Flow object
            period (int, optional): The WMA period. Defaults to 12

        Returns:
            (dict): WMA
        """
        return wma(flow, *args, **kwargs)

    @staticmethod
    def zigzag(flow: dict, *args, **kwargs) -> dict:
        return zigzag(flow, *args, **kwargs)