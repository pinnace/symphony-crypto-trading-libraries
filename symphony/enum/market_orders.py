from enum import Enum
from symphony.exceptions import EnumException


# TODO: Change name to MarketOrder
class Market(Enum):
    """
    Class containing constants for market orders
    """

    BUY = "buy"
    SELL = "sell"
    MARKET = "market"
    LIMIT = "limit"
    STOP_LIMIT = "stop_limit"
    TAKE_PROFIT_LIMIT = STOP_LIMIT
    STOP_LOSS_LIMIT = STOP_LIMIT
    STOP_MARKET = "stop_market"
    TAKE_PROFIT = "take_profit"
    LIMIT_MAKER = "limit_maker"



    @classmethod
    def get_order_type(cls, order_type: str) -> Enum:
        """
        Derives the order type from a string

        :param order_type: To lookup
        :return: Market enum
        :raises EnumException: If the order_type is unknown
        """
        for ot in cls:
            if order_type.lower() == "take_profit_limit" or order_type.lower() == "stop_loss_limit":
                return cls.STOP_LIMIT
            elif order_type.lower() == ot.value:
                return ot
        raise EnumException(f"Unknown order_type: {order_type}")


class OrderStatus(Enum):
    """
    Class containing constants for order status
    """
    OPEN = "open_order"
    FILLED = "filled"
    CANCELLED = "cancelled"
    PARTIALLY_FILLED = "partially_filled"

    @classmethod
    def get_status(cls, status: str) -> Enum:
        """
        Derives the status from a string

        :param status: To lookup
        :return: OrderStatus enum
        :raises EnumException: If the status is unknown
        """
        if status.lower() == cls.OPEN.value or status.lower() == "new" or status.lower() == "open":
            return cls.OPEN
        elif status.lower() == cls.FILLED.value or status.lower() == "closed":
            return cls.FILLED
        elif status.lower() == cls.CANCELLED.value or status.lower() == 'canceled':
            return cls.CANCELLED
        elif status.lower() == cls.PARTIALLY_FILLED.value:
            return cls.PARTIALLY_FILLED
        else:
            raise EnumException(f"Unknown status: {status}")

