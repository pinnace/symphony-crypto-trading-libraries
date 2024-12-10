from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute
from pynamodb_attributes import UnicodeEnumAttribute, IntegerAttribute, TimestampAttribute
from symphony.enum import OrderStatus, Exchange, Market, AccountType
from symphony.config import DYNAMODB_HOST, DYNAMODB_ORDERS_TABLE


class OrderModel(Model):

    class Meta:
        table_name = DYNAMODB_ORDERS_TABLE
        host = DYNAMODB_HOST
    symbol = UnicodeAttribute()
    base_asset = UnicodeAttribute()
    quote_asset = UnicodeAttribute()
    digits = IntegerAttribute()
    order_id = IntegerAttribute(hash_key=True)
    client_order_id = UnicodeAttribute()
    order_status = UnicodeEnumAttribute(OrderStatus)
    exchange = UnicodeAttribute()
    account_type = UnicodeEnumAttribute(AccountType)
    commission_amount = NumberAttribute()
    commission_asset = UnicodeAttribute(null=True)
    price = NumberAttribute()
    stop_price = NumberAttribute()
    quantity = NumberAttribute()
    filled_quantity = NumberAttribute()
    transacted_quantity = NumberAttribute()
    order_side = UnicodeEnumAttribute(Market)
    order_type = UnicodeEnumAttribute(Market)
    order_placed_time = TimestampAttribute()
    order_last_trade_time = TimestampAttribute(null=True)

