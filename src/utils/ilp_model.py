import json
import os
from typing import Any

import gurobipy as gp
from pandas import DataFrame

from src.utils import consts
from src.utils.functional_dependency import FunctionalDependency
from src.utils.marginal_calculator import Marginals


class ILPModel:
    def __init__(self, data: DataFrame, fds: list[FunctionalDependency], marginals: Marginals, config: dict[str, Any]):
        self.data = data
        self.fds = fds
        self.marginals = marginals
        self.config = config
        self.model = self.__create_model()

        indices = range(len(data))
        self.objective = self.model.addVars(indices, vtype=gp.GRB.BINARY, name=[f"x_{i}" for i in indices])

    def __create_model(self) -> gp.Model:
        license_file_name = self.config["license_file_name"]
        license_file_path = os.path.join(consts.LICENSES_DIR_PATH, license_file_name)
        license_params = json.load(open(license_file_path))
        env = gp.Env(params=license_params)
        return gp.Model("ILP", env=env)

    def solve(self, is_max=True) -> "ILPModel":
        self.model.setObjective(self.objective.sum(), gp.GRB.MAXIMIZE if is_max else gp.GRB.MINIMIZE)
        self.model.update()
        self.model.optimize()
        return self

    def did_succeed(self) -> bool:
        return self.model.status == gp.GRB.OPTIMAL
