from typing import Callable

import gurobipy as gp
from pandas import DataFrame

from src.ilp.optimal_data_repair_ilp import OptimalDataRepairILP
from src.marginals.marginals import Marginals
from src.utils.configuration import Configuration
from src.violations.functional_dependency import FunctionalDependency


class VertexCoverDataRepairILP(OptimalDataRepairILP):
    def __init__(self, data: DataFrame, fds: list[FunctionalDependency], config: Configuration, marginals: Marginals):
        super().__init__(data, fds, config)
        self.rounding_approach = lambda x: 0 if x < 0.5 else 1
        self.weight_function = self.__build_weight_function(marginals)

    @property
    def variable_type(self) -> str:
        return gp.GRB.CONTINUOUS

    def __add_normalization_constraint(self) -> None:
        for _, var in self.objective:
            self.model.addConstr(var >= 0)
            self.model.addConstr(var <= 1)

    def __build_weight_function(self, marginals: Marginals) -> Callable[[int], float]:
        weights = {i: self.__get_tuple_weight(i, marginals) for i in range(len(self.data))}
        return lambda i: weights[i]

    def __get_tuple_weight(self, tuple_index: int, marginals: Marginals) -> float:
        data_without_tuple = self.data.drop(index=tuple_index)
        marginals_without_tuple = Marginals(data_without_tuple)
        return marginals.mean_distance(marginals_without_tuple)

    def _set_model_objective(self) -> None:
        weighted_sum = gp.quicksum(self.weight_function(i) * self.objective[i] for i in range(len(self.data)))
        self.model.setObjective(weighted_sum, gp.GRB.MINIMIZE)

    @property
    def solution(self) -> dict[int, float]:
        return {i: self.rounding_approach(x.X) for i, x in self.objective.items()}
