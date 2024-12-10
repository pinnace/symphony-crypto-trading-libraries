from symphony.config import SLACK_WORKSPACE, SLACK_WEBHOOK_URL, LOG_LEVEL, SLACK_CHANNEL, SLACK_TOKEN, USE_MODIN
from symphony.data_classes import Order, Position, Instrument
from symphony.enum import Exchange, Timeframe, Market
from symphony.enum.timeframe import timeframe_to_string
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.web.slack_response import SlackResponse
import texttable
from typing import Optional, Union, List
import logging
logger = logging.getLogger(__name__)
log_level: logging = LOG_LEVEL
if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd

class SlackNotifier:

    def __init__(self, channel: Optional[str] = SLACK_CHANNEL, log_level: Optional[int] = LOG_LEVEL):
        """
        A Slack notification helper

        :param channel: Optionally override the channel in config.ini
        :param log_level: Optionally override log level
        """
        self.webhook = SLACK_WEBHOOK_URL
        self.token = SLACK_TOKEN
        self.channel = channel
        self.client = WebClient(token=self.token)

        logging.basicConfig(level=log_level)
        return

    @property
    def channel(self) -> str:
        return self.__channel

    @channel.setter
    def channel(self, channel: str):
        self.__channel = channel

    def notify_order(self, order: Order, timeframe: Optional[Timeframe] = None) -> SlackResponse:
        """
        Sends a message about an order to Slack

        :param order: The Order object
        :param timeframe: Optionally include the intended timeframe in the message
        :return: SlackResponse object
        """

        order_side = order.order_side.value.upper()
        table = texttable.Texttable()
        table.add_rows([
            ["Data", "Value"],
            ["Timestamp", str(order.timestamp)],
            ["Entry Price", order.price],
            ["Stop Loss", order.stop_price],
            ["Quantity", order.quantity]
        ])
        table.set_cols_align(["l", "r"])
        table.set_cols_dtype(["t", "f"])
        table.set_deco(texttable.Texttable.HEADER)
        markdown_block = f"""*Detail*:
        *Exchange*: {order.exchange.name.capitalize()}   {self.__get_exchange_icon(order.exchange)}
        *Instrument*: {order.instrument.symbol}
        *Account*: {order.account.value.upper()}
        *Side*: {order_side}   {':large_green_circle:' if order_side == "BUY" else ':red_circle:'}
        *Order Type*: {order.order_type.value}
        *Commission*: {order.commission_amount} {order.commission_asset}
        *Client ID*: {order.client_order_id}
        *Order ID*: {order.order_id}
        {f'*Timeframe*: {timeframe_to_string(timeframe)}' if not isinstance(timeframe, type(None)) else ''}
        """
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{markdown_block}\n\n```{table.draw()}```\n\n"
                }
            },
        ]
        response = self.client.chat_postMessage(
            channel=self.channel,
            blocks=blocks
        )
        return response

    def notify_position(self, position: Position, status: Optional[str] = "") -> SlackResponse:

        position_side = position.side.value.upper()
        table = texttable.Texttable()
        table.add_rows([
            ["Data", "Value"],
            ["Position Size", f"{position.position_size} {position.entry_denomination}"],
            ["Borrow Amount", f"{position.borrow_amount} {position.borrow_denomination}"],
            ["Margin Deposit", f"{position.margin_deposit} {position.deposit_denomination}"],
            ["Entry Value", f"{position.entry_value} {position.entry_denomination}"],
            ["Profit", f"{position.profit}"]
        ])
        table.set_cols_align(["l", "r"])
        table.set_deco(texttable.Texttable.HEADER)

        markdown_block = f"""*Position*:
        *Exchange*: {position.instrument.exchange.name.capitalize()}   {self.__get_exchange_icon(position.instrument.exchange)}
        *Status*: {status}
        *Instrument*: {position.instrument.symbol}
        *Side*: {position_side}   {':large_green_circle:' if position_side == "BUY" else ':red_circle:'}
        *Timeframe*: {timeframe_to_string(position.timeframe)}
        *Position ID*: {position.position_id}
        """
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{markdown_block}\n\n```{table.draw()}```\n\n"
                }
            },
        ]
        response = self.client.chat_postMessage(
            channel=self.channel,
            blocks=blocks
        )
        return response

    def notify_message(self, string_or_strings: Union[str, List[str]], channel: Optional[str] = "", is_markdown: Optional[bool] = False) -> Union[SlackResponse, List[SlackResponse]]:
        """
        Sends a string or list of strings to Slack

        :param string_or_strings: A single or list of messages
        :param is_markdown: If the text is markdown
        :return: Either a single or list of SlackResponse objects
        """
        responses = []
        strings = string_or_strings if isinstance(string_or_strings, list) else [string_or_strings]
        if not channel:
            channel = self.channel
        if not is_markdown:
            for string in strings:
                response = self.client.chat_postMessage(
                    channel=channel,
                    text=f"{string}"
                )
                responses.append(response)
        else:
            blocks = []
            for string in strings:
                blocks.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"{string}"
                        }
                    },
                )

            response = self.client.chat_postMessage(
                channel=channel,
                blocks=blocks
            )
            responses.append(response)

        return responses[0] if len(responses) == 1 else responses

    def notify_signal(self, strategy: str, timeframe: Timeframe, instrument: Instrument, time: pd.Timestamp, stop_loss: float, take_profit: float, side: Market, confidence: Optional[float] = -1, channel: Optional[str] = ""):
        """
        Posts notification about a trading signal

        :param strategy: Strategy name
        :param timeframe: Timeframe
        :param instrument: Instrument
        :param time: Time of signal
        :param stop_loss: Stop loss
        :param take_profit: Take profit
        :param side: BUY or SELL
        :param confidence: Model confidence
        :param channel: Optionally override default channel
        :return: Slack response object
        """
        markdown_block = f"""*{strategy}*
        *Timeframe*: {timeframe_to_string(timeframe)}
        *Symbol*: {instrument.symbol}
        *Time*: {str(time)}
        *Side*: {side.name}
        *Stop Loss*: {stop_loss}
        *Take Profit*: {take_profit}
        *Confidence*: {confidence}
        """
        if not channel:
            channel = self.channel
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{markdown_block}"
                }
            },
        ]
        response = self.client.chat_postMessage(
            channel=channel,
            blocks=blocks
        )
        return response



    def set_channel(self, channel: str) -> None:
        self.channel = channel

    def __get_exchange_icon(self, exchange: Exchange) -> str:
        """
        Get the custom icon for an exchange

        :param exchange: Exchange
        :return: markdown string
        """
        if exchange == Exchange.BINANCE:
            return ":binance:"
        return ""
