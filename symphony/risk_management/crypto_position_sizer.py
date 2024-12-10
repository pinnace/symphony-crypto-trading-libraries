from typing import List, Optional, Union, Callable, Tuple, SupportsRound
from symphony.data_classes import Instrument, filter_instruments
from symphony.enum import Exchange, Market, StableCoin, ExchangeType
from symphony.config import LOG_LEVEL
from symphony.exceptions import RiskManagementException
from symphony.client import ClientFactory
from symphony.abc import RealTimeQuoter, HistoricalQuoter
from symphony.data_classes import ConversionChain
from collections import deque
import itertools
import logging

logger = logging.getLogger(__name__)


class CryptoPositionSizer:

    def __init__(self,
                 quoter: Union[RealTimeQuoter, HistoricalQuoter],
                 maker_commission: Optional[float] = 0.0,
                 taker_commission: Optional[float] = 0.0,
                 log_level: Optional[int] = LOG_LEVEL
                 ):

        self.quoter: RealTimeQuoter = quoter
        self.exchange: Exchange = self.quoter.exchange
        self.exchange_client = ClientFactory.factory(self.exchange)
        if self.exchange.exchange_type != ExchangeType.CRYPTO:
            raise RiskManagementException(f"This exchange is not a CRYPTO exchange! "
                                          f"Use another position sizer. Exchange: {self.exchange.name}")

        self.exchange_instruments: List[Instrument] = self.exchange_client.get_all_instruments()
        self.maker_commission: float = maker_commission
        self.taker_commission: float = taker_commission
        self.conversion_chain: ConversionChain = ConversionChain(quoter)
        logger.setLevel(log_level)

    @staticmethod
    def simple_position_size(
            order_type: Market,
            entry_price: float,
            stop_loss: float,
            account_size: float,
            risk_perc: float,
            digits: float
    ) -> float:
        """
        Calculates a simple position size

        :param order_type: BUY or SELL
        :param entry_price: Order entry price
        :param stop_loss: Order stop loss
        :param account_size: Current account size
        :param risk_perc: Percentage at risk
        :param digits: Rounding digits
        :return: Position size
        """
        if order_type not in [Market.BUY, Market.SELL]:
            raise RiskManagementException(f"Order type must be BUY or SELL: {order_type}")
        amount_at_risk = account_size * risk_perc

        if order_type == Market.BUY:
            stop_perc = (entry_price - stop_loss) / entry_price
        else:
            stop_perc = (stop_loss - entry_price) / entry_price

        position_size: float = amount_at_risk / stop_perc
        return float(round(position_size, digits))

    def calculate_position_size(self,
                                target_instrument: Instrument,
                                order_type: Market,
                                entry_price: float,
                                stop_loss: float,
                                account_size: float,
                                account_denomination: str,
                                risk_perc: float,
                                margin: Optional[bool] = False,
                                fall_back_to_api: Optional[bool] = False
                                ) -> float:
        """
        Calculates a position size if provided with the following parameters.

        :param target_instrument: The Instrument you want to trade
        :param order_type: The order type. Either BUY or SELL
        :param entry_price: The order entry price
        :param stop_loss: Stop loss level
        :param account_size: Total account size
        :param account_denomination: Account size denomination
        :param risk_perc: Amount at risk as a percentage
        :param margin: Optionally provide if you are trading on margin
        :param fall_back_to_api: If you provided a RealTimeQuoter, fall back to API if no quotes have been pushed yet
        :return: Position size as a float
        :raises RiskManagementException: If amount at risk is too large, if shorting without margin, if stop levels dont
                                            make sense for order type, if order type is unknown
        """
        if not margin and risk_perc > 1.0:
            raise RiskManagementException(f"% at risk is too large, {risk_perc}")
        if order_type not in [Market.BUY, Market.SELL]:
            raise RiskManagementException(f"Order type must be BUY or SELL: {order_type}")
        if not margin and order_type == Market.SELL:
            raise RiskManagementException("Cannot short without margin enabled")
        if order_type == Market.BUY and stop_loss >= entry_price:
            raise RiskManagementException("For BUY stop loss cannot be above entry price")
        if order_type == Market.SELL and stop_loss <= entry_price:
            raise RiskManagementException("For SELL stop loss cannot be below entry price")

        amount_at_risk = account_size * risk_perc

        if order_type == Market.BUY:
            stop_perc = (entry_price - stop_loss) / entry_price
        elif order_type == Market.SELL:
            stop_perc = (stop_loss - entry_price) / entry_price

        if entry_price and (account_denomination == target_instrument.base_asset or account_denomination == target_instrument.quote_asset):
            if account_denomination == target_instrument.base_asset:
                pass
            else:
                amount_at_risk = amount_at_risk / entry_price
        else:
            amount_at_risk = self.conversion_chain.convert(account_denomination,
                                                           target_instrument.base_asset,
                                                           amount=amount_at_risk,
                                                           order_type=order_type,
                                                           fall_back_to_api=fall_back_to_api)
        position_size = amount_at_risk / stop_perc

        return round(position_size, target_instrument.digits)

    def smart_margin(self,
                     instrument: Instrument,
                     price: float,
                     stop_loss: float,
                     position_size: float,
                     order_side: Market,
                     margin_ratio: int,
                     margin_level: Optional[float] = 1.3) -> Tuple[str, float]:
        """
        Calculates the required margin amount needed to stay above margin_level even if stop loss is hit

        :param instrument: Instrument to be traded
        :param price: Entry price
        :param stop_loss: Stop loss
        :param position_size: Position size in terms of base asset
        :param order_side: BUY or SELL
        :param margin_level: Desired minimum margin level (i.e. margin level when stop loss would be hit)
        :param margin_ratio: Instrument margin ratio
        :return: The deposit currency and margin amount
        """
        if order_side not in [Market.BUY, Market.SELL]:
            raise RiskManagementException(f"Unknown order side: {order_side}")
        if order_side == Market.SELL and stop_loss < price:
            raise RiskManagementException(f"SL must be more than price for SELL")
        if order_side == Market.BUY and price < stop_loss:
            raise RiskManagementException(f"SL must be less than price for BUY")

        # TODO: This can be better optimized
        def check_min_margin(margin_required) -> float:
            if margin_required + (margin_required * (margin_ratio - 1)) < position_size:
                return position_size / (margin_ratio - 1)
            return margin_required

        if order_side == Market.BUY:
            # Don't ask me how this is derived
            # https://www.wolframalpha.com/input/?i=(((b/p)+++(z/x))*s)+/+b+=+l,+b+=+(((z/x)+*+((z/x)+/+z))+-+(z/x))+*+p,+solve+for+x
            x = 1 - (stop_loss / (margin_level * price))
            margin_required = position_size * x
            margin_required = check_min_margin(margin_required)
            return instrument.base_asset, round(margin_required, instrument.digits)
        else:
            # https://www.wolframalpha.com/input/?i=(((b*p)+++(z/x))/s)+/+b+=+l,+b+=+(((z/x)+*+((z/x)+/+z))+-+(z/x))+/+p,+solve+for+x
            position_size = position_size * price
            x = 1 - (price / (margin_level * stop_loss))
            margin_required = position_size * x
            margin_required = check_min_margin(margin_required)
            return instrument.quote_asset, round(margin_required, instrument.digits)

