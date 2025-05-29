from abc import ABC
from typing import Callable

from pandas.core.interchange.dataframe_protocol import DataFrame

from src.marginals.marginals import Marginals


class CostFunction(ABC):
    def calculate_cost(self, data: DataFrame, marginals: Marginals) -> Callable[[int], float]:
        raise NotImplementedError("Cost function was not implemented")