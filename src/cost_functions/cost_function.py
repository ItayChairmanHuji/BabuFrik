from abc import ABC

from pandas.core.interchange.dataframe_protocol import DataFrame

from src.marginals.marginals_constraints import MarginalsConstraints


class CostFunction(ABC):
    def calculate_cost(self, data: DataFrame, marginals: MarginalsConstraints) -> dict[int, float]:
        raise NotImplementedError("Cost function was not implemented")
