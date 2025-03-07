from typing import Any

import gurobipy as gp
from pandas import DataFrame

from src.utils import consts
from src.utils.constraint_adders.marginals_constraint_adder import MarginalsConstraintAdder
from src.utils.constraint_adders.trivial_solution_constraint_adder import TrivialSolutionConstraintAdder
from src.utils.constraint_adders.violations_constraint_adder import ViolationsConstraintAdder
from src.utils.functional_dependency import FunctionalDependency, load_fds
from src.utils.ilp_model import ILPModel
from src.utils.marginal_calculator import Marginals
from src.utils.node import Node


class ILPRepairer(Node):
    ILPSolution = gp.tupledict[str, gp.Var]

    def __init__(self, config: dict[str, Any]):
        super().__init__(config=config,
                         fields=["license_file_name", "error_in_marginals"])

    @staticmethod
    def output_file_path() -> str:
        return consts.REPAIRED_DATA_FILE_NAME

    def node_action(self, data: DataFrame) -> DataFrame:
        fds = load_fds(self.working_dir)
        marginals = Marginals(self.working_dir)
        ilp = self.__build_ilp(data, fds, marginals)
        ilp.solve()
        return self.__get_feasible_solution(data, ilp) if ilp.did_succeed() else self.__get_feasible_solution(data, ilp)

    def __build_ilp(self, data: DataFrame, fds: list[FunctionalDependency], marginals: Marginals) -> ILPModel:
        return TrivialSolutionConstraintAdder.add_constraint(
            MarginalsConstraintAdder.add_constraint(
                ViolationsConstraintAdder.add_constraint(
                    ILPModel(data, fds, marginals, self.config)
                )
            )
        )

    @staticmethod
    def __get_infeasible_solution(data: DataFrame) -> DataFrame:
        print("Model is infeasible")
        return data.drop(index=data.index)

    @staticmethod
    def __get_feasible_solution(data: DataFrame, ilp: ILPModel) -> DataFrame:
        tuples_to_remove = [i for i, x in enumerate(ilp.objective) if x == 0]
        return data.drop(index=tuples_to_remove)
