import json
import os

import gurobipy as gp
from pandas import DataFrame

from src.utils import consts
from src.utils.configuration import Configuration
from src.violations import violations_checker
from src.violations.functional_dependency import FunctionalDependency


class OptimalDataRepairILP:
    def __init__(self, data: DataFrame, fds: list[FunctionalDependency], config: Configuration, *args):
        self.data = data
        self.fds = fds
        self.config = config
        self.model = self.__create_model()
        self.objective = self.model.addVars(range(len(data)),
                                            vtype=gp.GRB.BINARY, name=[f"x_{i}" for i in range(len(data))])
        self.__add_no_trivial_solution_constraint()
        self.violating_tuples = violations_checker.find_violating_pairs(self.data, self.fds)

    def __create_model(self) -> gp.Model:
        license_file_name = self.config["license_file_name"]
        license_file_path = os.path.join(consts.LICENSES_DIR_PATH, license_file_name)
        license_params = json.load(open(license_file_path))
        env = gp.Env(params=license_params)
        model = gp.Model("ILP", env=env)
        model.setParam('OutputFlag', False)
        return model

    def __add_no_trivial_solution_constraint(self) -> None:
        self.model.addConstr(self.objective.sum() >= 1)

    def __add_no_violations_constraint(self, model: gp.Model, where: int) -> None:
        if where == gp.GRB.Callback.MIPSOL:
            x = model.cbGetSolution(self.objective)
            for i, j in self.violating_tuples:
                if x[i] + x[j] > 1: model.cbLazy(self.objective[i] + self.objective[j] <= 1)

    def solve(self) -> "OptimalDataRepairILP":
        self.model.params.LazyConstraints = True
        self._set_model_objective()
        self.model.update()
        self.model.optimize(self.__add_no_violations_constraint)
        return self

    def _set_model_objective(self) -> None:
        self.model.setObjective(self.objective.sum(), gp.GRB.MAXIMIZE)

    @property
    def did_succeed(self) -> bool:
        return self.model.status == gp.GRB.OPTIMAL

    @property
    def solution(self) -> dict[int, float]:
        return {i: x.X for i, x in self.objective.items()}
