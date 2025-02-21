import json
import os.path
import pickle
from functools import reduce
from typing import Any

import gurobipy as gp
from pandas import DataFrame

from functional_dependency import FunctionalDependency
from src.utils import consts
from src.utils.marginal_calculator import MarginalsCalculator
from src.utils.node import Node
from src.utils.violations_checker import ViolationsChecker


class ILPRepairer(Node):
    ILPSolution = gp.tupledict[str, gp.Var]

    def __init__(self, config: dict[str, Any]):
        super().__init__(config=config,
                         fields=["license_file_name", "error_in_marginals"])

    @staticmethod
    def name():
        return "ilp_repairer"

    @staticmethod
    def output_file_path() -> str:
        return consts.REPAIRED_DATA_FILE_NAME

    def node_action(self, data: DataFrame) -> DataFrame:
        fds = self.__load_fds()
        marginals = self.__load_marginals()
        model = self.__build_model(data, fds, marginals)
        model.optimize()
        if model.status == gp.GRB.Status.INFEASIBLE:
            print("Model is infeasible")
            return data.drop(index=data.index)
        tuples_to_remove = [i for i,x in enumerate(model.X) if x == 0]
        return data.drop(index=tuples_to_remove)

    def __load_fds(self) -> list[FunctionalDependency]:
        fd_file = os.path.join(self.working_dir, consts.FUNCTIONAL_DEPENDENCIES_FILE_NAME)
        fds_as_json = json.load(open(fd_file))
        return [FunctionalDependency(source=fd["source"], target=fd["target"]) for fd in fds_as_json]

    def __load_marginals(self) -> MarginalsCalculator.MarginalsSet:
        marginals_file_path = os.path.join(self.working_dir, consts.MARGINALS_FILE_NAME)
        marginals_resource_file_path = os.path.join(consts.RESOURCES_DIR_PATH, consts.MARGINALS_FILE_NAME)
        return pickle.load(open(marginals_file_path, "rb")) if os.path.exists(marginals_file_path) \
            else pickle.load(open(marginals_resource_file_path, "rb"))

    def __build_model(self, data: DataFrame, fds: list[FunctionalDependency],
                      marginals: MarginalsCalculator.MarginalsSet) -> gp.Model:
        number_of_tuples = len(data)
        model = self.__generate_model()
        objective = model.addVars(range(number_of_tuples), vtype=gp.GRB.BINARY,
                                  name=[f"x_{i}" for i in range(number_of_tuples)])
        self.__add_violations_constraints(data, fds, model, objective)
        self.__add_marginals_constraints(data, marginals, model, objective)
        self.__add_trivial_solution_constraint(model, objective)
        model.setObjective(objective.sum(), gp.GRB.MAXIMIZE)
        model.update()
        return model

    def __generate_model(self) -> gp.Model:
        license_file_path = os.path.join(consts.LICENSES_DIR_PATH, self.config["license_file_name"])
        license_params = json.load(open(license_file_path))
        env = gp.Env(params=license_params)
        return gp.Model("ILP", env=env)

    @staticmethod
    def __add_violations_constraints(data: DataFrame, fds: list[FunctionalDependency], model: gp.Model,
                                     objective: ILPSolution) -> None:
        violating_pairs = ViolationsChecker.find_violating_pairs(data, fds)
        for pair in violating_pairs:
            model.addConstr(gp.quicksum(objective[i] for i in pair) <= 1)

    def __add_marginals_constraints(self, data: DataFrame, marginals: MarginalsCalculator.MarginalsSet,
                                    model: gp.Model, objective: ILPSolution) -> None:
        error_in_marginals = self.config["error_in_marginals"]
        for attributes_pair, attributes_marginals in marginals.items():
            for values_pair, values_marginals in attributes_marginals.items():
                indices = data[reduce(lambda acc, pair: acc & data[pair[0]].isin([pair[1]]),
                                      zip(attributes_pair, values_pair), True)].index
                model.addConstr(
                    gp.quicksum(objective[i] for i in indices) <= (
                            values_marginals + error_in_marginals) * objective.sum(),
                    name=f"marginal_upper_{attributes_pair}_{values_pair}"
                )
                model.addConstr(
                    gp.quicksum(objective[i] for i in indices) >= (
                            values_marginals - error_in_marginals) * objective.sum(),
                    name=f"marginal_lower_{attributes_pair}_{values_pair}"
                )

    @staticmethod
    def __add_trivial_solution_constraint(model: gp.Model, objective: ILPSolution) -> None:
        model.addConstr(objective.sum() >= 1)
