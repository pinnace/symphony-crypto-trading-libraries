from symphony.enum import Exchange, AccountType, OrderStatus, Market
from symphony.data_classes.order import Order, Instrument
from symphony.models import OrderModel
from symphony.config import USE_MODIN
from symphony.exceptions import UtilsException
from typing import Dict, Optional, Union

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


def order_from_binance_api(order: Dict[str, str], instrument: Instrument, account_type: AccountType) -> Order:
    """
    Builds Order from Binance API

    :param order: API response
    :param instrument: Order instrument
    :param account_type: The account we are trading on [SPOT, MARGIN, ISOLATED_MARGIN]
    :return: Order
    """

    new_order = Order()
    new_order.exchange = Exchange.BINANCE
    new_order.account = account_type
    new_order.order_id = int(order["orderId"])
    new_order.client_order_id = str(order["clientOrderId"])
    new_order.price = float(order["price"])
    new_order.instrument = instrument
    new_order.stop_price = float(order["stopPrice"])
    new_order.quantity = float(order["origQty"])
    new_order.transacted_quantity = float(order["cummulativeQuoteQty"])
    new_order.filled = float(order['executedQty'])
    new_order.status = OrderStatus.get_status(order["status"])
    new_order.timestamp = pd.Timestamp(order["time"], unit='ms', tz='UTC')
    new_order.last_traded_timestamp = pd.Timestamp(order["updateTime"], unit='ms', tz='UTC')
    new_order.order_side = Market.get_order_type(order["side"])
    new_order.order_type = Market.get_order_type(order["type"])
    return new_order


def order_from_binance_websocket(order: Dict[str, str], instrument: Instrument, account_type: AccountType) -> Order:
    """
    Build Order from Binance WebSocket message

    :param order: API response
    :param instrument: Order instrument
    :param account_type: The account we are trading on [SPOT, MARGIN, ISOLATED_MARGIN]
    :return: Order
    """

    new_order = Order()
    new_order.exchange = Exchange.BINANCE
    new_order.account = account_type
    new_order.instrument = instrument
    new_order.order_id = int(order["i"])
    new_order.client_order_id = str(order["c"])
    new_order.status = OrderStatus.get_status(order["X"])
    new_order.order_side = Market.get_order_type(order["S"])
    new_order.order_type = Market.get_order_type(order["o"])
    if new_order.status == OrderStatus.OPEN:
        new_order.price = float(order["p"])
    else:
        if new_order.order_type == Market.MARKET:
            new_order.price = float(order["L"])
        else:
            new_order.price = 0.0 if float(order["z"]) == 0.0 \
                else round(float(order["Z"]) / float(order["z"]), instrument.digits)
    new_order.stop_price = float(order["P"])
    new_order.quantity = float(order["q"])
    new_order.transacted_quantity = float(order["Z"])
    new_order.filled = float(order["z"])
    new_order.timestamp = pd.Timestamp(order["T"], unit='ms', tz='UTC')
    new_order.last_traded_timestamp = pd.Timestamp(order["O"], unit='ms', tz='UTC')
    new_order.commission_amount = float(order["n"])
    new_order.commission_asset = None if isinstance(order["N"], type(None)) else str(order["N"])
    return new_order


def order_from_cctx(
        resp: Dict[str, str],
        exchange: Exchange,
        instrument: Instrument,
        account_type: AccountType
) -> Order:
    """
    Converts ccxt response to an Order object

    :param resp: CCXT response
    :param exchange: Exchange we are trading on
    :param instrument: Instrument we are trading
    :param account_type: The account we are trading on [SPOT, MARGIN, ISOLATED_MARGIN]
    :return:
    """
    new_order = Order()
    new_order.exchange = exchange
    new_order.account = account_type
    new_order.instrument = instrument
    new_order.order_id = int(resp["id"])
    new_order.status = OrderStatus.get_status(resp['status'])
    new_order.timestamp = pd.Timestamp(resp['timestamp'], unit='ms', tz='UTC')
    new_order.last_traded_timestamp = None if isinstance(resp['lastTradeTimestamp'], type(None)) \
        else pd.Timestamp(resp['lastTradeTimestamp'], unit='ms', tz='UTC')
    new_order.client_order_id = str(resp['clientOrderId'])
    if not isinstance(resp['fee'], type(None)):
        new_order.commission_amount = float(resp['fee']['cost'])
        new_order.commission_asset = str(resp['fee']['currency'])
    else:
        new_order.commission_amount = 0.0
        new_order.commission_asset = None
    new_order.price = float(resp['price'])
    new_order.quantity = float(resp['amount'])
    new_order.filled = float(resp['filled'])
    new_order.transacted_quantity = float(resp['cost'])
    new_order.order_side = Market.get_order_type(resp['side'])
    new_order.order_type = Market.get_order_type(resp['type'])
    if isinstance(resp['stopPrice'], type(None)):
        # Happens for STOP_LOSS_LIMIT
        new_order.stop_price = 0.0
    else:
        new_order.stop_price = float(resp['stopPrice'])
    return new_order


def order_model_from_order(order: Order) -> OrderModel:
    """
    Converts order object to pynamodb OrderModel

    :param order: The Order object
    :return: OrderModel
    :raises UtilsException: If the order is not an Order
    """
    if not isinstance(order, Order):
        raise UtilsException(f"order is not an instance of Order. Type: {type(order)}")
    new_order_model = OrderModel()
    new_order_model.account_type = order.account
    new_order_model.symbol = order.instrument.symbol
    new_order_model.base_asset = order.instrument.base_asset
    new_order_model.quote_asset = order.instrument.quote_asset
    new_order_model.digits = order.instrument.digits
    new_order_model.order_id = order.order_id
    new_order_model.client_order_id = order.client_order_id
    new_order_model.order_status = order.status
    new_order_model.exchange = order.exchange.name
    new_order_model.commission_amount = order.commission_amount
    new_order_model.commission_asset = order.commission_asset
    new_order_model.price = order.price
    new_order_model.stop_price = order.stop_price
    new_order_model.quantity = order.quantity
    new_order_model.transacted_quantity = order.transacted_quantity
    new_order_model.filled_quantity = order.filled
    new_order_model.order_side = order.order_side
    new_order_model.order_type = order.order_type
    new_order_model.order_placed_time = order.timestamp
    new_order_model.order_last_trade_time = order.last_traded_timestamp
    return new_order_model


def insert_or_update_order(order_or_order_model: Union[OrderModel, Order], create_table: Optional[bool] = True) -> None:
    """
    Inserts or updates the order in DynamoDB. Deletes cancelled orders. Orders are keyed to the exchange order_id

    :param order_or_order_model: OrderModel or Order instance
    :param create_table: Optionally create the table if it does not exist, default to True
    :return: None
    :raises UtilsException: If the operation failed to complete
    """
    if isinstance(order_or_order_model, Order):
        order_model = order_model_from_order(order_or_order_model)
    elif isinstance(order_or_order_model, OrderModel):
        order_model = order_or_order_model
    else:
        raise UtilsException(f"Unknown type: {type(order_or_order_model)}")

    if create_table and not order_model.exists():
        order_model.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
    try:
        if order_model.order_status == OrderStatus.CANCELLED:
            order_model.delete()
        else:
            order_model.save()
    except Exception as e:
        raise UtilsException(f"Failed to write order. Error: {str(e)}")
    return

def remove_order(order_model: OrderModel) -> None:
    """
    Removes order from configured database

    :param order_model:
    :return:
    """