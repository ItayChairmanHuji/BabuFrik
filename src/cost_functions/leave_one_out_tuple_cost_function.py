from typing import Callable

from pandas import DataFrame

from src.marginals.marginals import Marginals


class LeaveOneOutTupleCostFunction:
    def calculate_cost(self, data: DataFrame, marginals: Marginals) -> Callable[[int], float]:
        weights = {i: self.get_tuple_cost(data, i, marginals) for i in range(len(data))}
        return lambda i: weights[i]

    def get_tuple_cost(self, data: DataFrame, tuple_index: int, marginals: Marginals) -> float:
        data_without_tuple = data.drop(index=tuple_index)
        marginals_without_tuple = Marginals(data_without_tuple)
        return marginals.mean_distance(marginals_without_tuple)
