from typing import Callable

import gurobipy as gp
from pandas import DataFrame

from src.ilp.optimal_data_repair_ilp import OptimalDataRepairILP
from src.marginals.marginals import Marginals
from src.utils.configuration import Configuration
from src.utils.functional_dependency import FunctionalDependency


class VertexCoverDataRepairILP(OptimalDataRepairILP):
    def __init__(self, data: DataFrame, fds: list[FunctionalDependency], config: Configuration, marginals: Marginals):
        super().__init__(data, fds, config)

        for var in self.objective.values():
            var.VType = gp.GRB.CONTINUOUS

        self.rounding_approach = lambda x: 0 if x < 0.5 else 1
        self.weight_function = self.__build_weight_function(marginals)

    def __build_weight_function(self, marginals: Marginals) -> Callable[[int], float]:
        weights = {i: self.__get_tuple_weight(i, marginals) for i in range(len(self.data))}
        return lambda i: weights[i]

    def __get_tuple_weight(self, tuple_index: int, marginals: Marginals) -> float:
        data_without_tuple = self.data.drop(index=tuple_index)
        marginals_without_tuple = Marginals(data_without_tuple)
        return marginals.distance(marginals_without_tuple)

    def __set_model_objective(self) -> None:
        weighted_sum = gp.quicksum(self.weight_function(i) * self.objective[i] for i in range(len(self.data)))
        self.model.setObjective(weighted_sum, gp.GRB.MAXIMIZE)

    @property
    def solution(self) -> iter:
        should_round = self.rounding_approach is not None
        yield from map(lambda x: self.rounding_approach(x) if should_round else x, self.objective)
