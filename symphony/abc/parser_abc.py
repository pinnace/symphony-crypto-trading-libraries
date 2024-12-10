from abc import ABC, abstractmethod
from typing import List
import pandas


class ParserABC(ABC):

    @abstractmethod
    def parse(self, **kwargs) -> pandas.DataFrame:
        pass