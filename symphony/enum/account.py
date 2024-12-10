from enum import Enum


class AccountType(Enum):
    SPOT = "spot"
    MARGIN = "margin"
    ISOLATED_MARGIN = "isolated_margin"


class BalanceType(Enum):
    FREE = "free"
    LOCKED = "locked"
    BORROWED = "borrowed"
    INTEREST = "interest"
    NET = "net"
    NET_BTC = "netBtc"
