from dataclasses import dataclass
from typing import Optional


@dataclass
class MarginAccount:

    def __init__(self,
                 trade_enabled: Optional[bool] = False,
                 transfer_enabled: Optional[bool] = False,
                 borrow_enabled: Optional[bool] = False,
                 margin_level: Optional[float] = 0.0,
                 margin_ratio: Optional[int] = 0,
                 total_asset_denomination: Optional[str] = "BTC",
                 total_assets: Optional[float] = 0.0,
                 total_liability: Optional[float] = 0.0,
                 total_net: Optional[float] = 0.0
                 ):
        self.trade_enabled: bool = trade_enabled
        self.transfer_enabled: bool = transfer_enabled
        self.borrow_enabled: bool = borrow_enabled
        self.margin_level: float = margin_level
        self.margin_ratio: int = margin_ratio
        self.total_asset_denomination: str = total_asset_denomination
        self.total_assets: float = total_assets
        self.total_liability: float = total_liability
        self.total_net: float = total_net

    @property
    def trade_enabled(self) -> bool:
        return self.__trade_enabled

    @trade_enabled.setter
    def trade_enabled(self, trade_enabled: bool):
        self.__trade_enabled = trade_enabled

    @property
    def transfer_enabled(self) -> bool:
        return self.__transfer_enabled

    @transfer_enabled.setter
    def transfer_enabled(self, transfer_enabled: bool):
        self.__transfer_enabled = transfer_enabled

    @property
    def borrow_enabled(self) -> bool:
        return self.__borrow_enabled

    @borrow_enabled.setter
    def borrow_enabled(self, borrow_enabled: bool):
        self.__borrow_enabled = borrow_enabled

    @property
    def margin_level(self) -> float:
        return self.__margin_level

    @margin_level.setter
    def margin_level(self, margin_level: float):
        self.__margin_level = margin_level

    @property
    def margin_ratio(self) -> int:
        return self.__margin_ratio

    @margin_ratio.setter
    def margin_ratio(self, margin_ratio: int):
        self.__margin_ratio = margin_ratio

    @property
    def total_asset_denomination(self) -> str:
        return self.__total_asset_denomination

    @total_asset_denomination.setter
    def total_asset_denomination(self, total_asset_denomination: str):
        self.__total_asset_denomination = total_asset_denomination

    @property
    def total_assets(self) -> float:
        return self.__total_assets

    @total_assets.setter
    def total_assets(self, total_assets: float):
        self.__total_assets = total_assets

    @property
    def total_liability(self) -> float:
        return self.__total_liability

    @total_liability.setter
    def total_liability(self, total_liability: float):
        self.__total_liability = total_liability

    @property
    def total_net(self) -> float:
        return self.__total_net

    @total_net.setter
    def total_net(self, total_net: float):
        self.__total_net = total_net

