from pandas import DataFrame

from src.marginals.marginals_constraints import MarginalsConstraints


class LeaveOneOutTupleCostFunction:
    def calculate_cost(self, data: DataFrame, marginals: MarginalsConstraints) -> dict[int, float]:
        return {i: self.get_tuple_cost(data, i, marginals) for i in range(len(data))}

    @staticmethod
    def get_tuple_cost(data: DataFrame, tuple_index: int, marginals: MarginalsConstraints) -> float:
        data_without_tuple = data.drop(index=tuple_index)
        return marginals.count_satisfied(data_without_tuple)
