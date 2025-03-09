import json
import os

import gurobipy as gp
from pandas import DataFrame

from src.utils import consts
from src.utils.configuration import Configuration
from src.violations import violations_checker
from src.violations.functional_dependency import FunctionalDependency


class OptimalDataRepairILP:
    def __init__(self, data: DataFrame, fds: list[FunctionalDependency], config: Configuration):
        self.data = data
        self.fds = fds
        self.config = config
        self.model = self.__create_model()
        self.objective = self.model.addVars(range(len(data)),
                                            vtype=gp.GRB.BINARY, name=[f"x_{i}" for i in range(len(data))])
        self.__add_no_violations_constraint()
        self.__add_no_trivial_solution_constraint()

    def __create_model(self) -> gp.Model:
        license_file_name = self.config["license_file_name"]
        license_file_path = os.path.join(consts.LICENSES_DIR_PATH, license_file_name)
        license_params = json.load(open(license_file_path))
        env = gp.Env(params=license_params)
        return gp.Model("ILP", env=env)

    def __add_no_trivial_solution_constraint(self) -> None:
        self.model.addConstr(self.objective.sum() >= 1)

    def __add_no_violations_constraint(self) -> None:
        violating_pairs = violations_checker.find_violating_pairs(self.data, self.fds)
        self.model.addConstrs((gp.quicksum(self.objective[i] for i in pair) <= 1
                               for pair in violating_pairs), name="violations constraints")

    def solve(self) -> "OptimalDataRepairILP":
        self.__set_model_objective()
        self.model.update()
        self.model.optimize()
        return self

    def __set_model_objective(self) -> None:
        self.model.setObjective(self.objective.sum(), gp.GRB.MINIMIZE)

    @property
    def did_succeed(self) -> bool:
        return self.model.status == gp.GRB.OPTIMAL

    @property
    def solution(self) -> iter:
        yield from self.objective
