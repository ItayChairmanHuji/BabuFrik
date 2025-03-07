import json
import os
from typing import Any, Callable

import gurobipy as gp
from pandas import DataFrame

from src.utils import consts
from src.utils.functional_dependency import FunctionalDependency
from src.utils.marginal_calculator import Marginals


class ILPModel:
    def __init__(self, data: DataFrame, fds: list[FunctionalDependency], marginals: Marginals, config: dict[str, Any],
                 rounding_approach: Callable[[float], int] = None):
        self.data = data
        self.fds = fds
        self.marginals = marginals
        self.config = config
        self.model = self.__create_model()
        self.rounding_approach = rounding_approach
        self.weight_function = lambda x: 1

        indices = range(len(data))
        objective_type = gp.GRB.BINARY if self.rounding_approach is None else gp.GRB.CONTINUOUS
        self.objective = self.model.addVars(indices, vtype=objective_type, name=[f"x_{i}" for i in indices])

    def __create_model(self) -> gp.Model:
        license_file_name = self.config["license_file_name"]
        license_file_path = os.path.join(consts.LICENSES_DIR_PATH, license_file_name)
        license_params = json.load(open(license_file_path))
        env = gp.Env(params=license_params)
        return gp.Model("ILP", env=env)

    def solve(self,is_max=True) -> "ILPModel":
        weighted_sum = gp.quicksum(self.weight_function(i) * self.objective[i] for i in range(len(self.data)))
        print("Weighted sum is:", weighted_sum)
        for i in range(len(self.data)):
            print(self.weight_function(i))
        optimization_operation = gp.GRB.MAXIMIZE if is_max else gp.GRB.MINIMIZE
        self.model.setObjective(weighted_sum, optimization_operation)
        self.model.update()
        self.model.optimize()
        return self

    @property
    def did_succeed(self) -> bool:
        return self.model.status == gp.GRB.OPTIMAL

    @property
    def solution(self) -> iter:
        should_round = self.rounding_approach is not None
        yield from map(lambda x: self.rounding_approach(x) if should_round else x, self.objective)
