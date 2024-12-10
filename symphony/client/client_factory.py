from symphony.enum import Exchange
from .binance_client import BinanceClient
from symphony.abc import ClientABC
from symphony.config import LOG_LEVEL
from symphony.exceptions import ClientClassException
from typing import Union, Type


class ClientFactory:

    @staticmethod
    def factory(client_type: Union[Exchange]) -> Type[ClientABC]:
        """
        Returns an instance of a client. Currently only Exchange clients
        supported. Can support more in the future

        :param client_type: The type of client
        :return: An instantiated client
        :raises ClientClassException: If the client type is unknown
        """

        if client_type == Exchange.BINANCE:
            return BinanceClient(log_level=LOG_LEVEL)
        else:
            raise ClientClassException(f"Could not instantiate {client_type}")
