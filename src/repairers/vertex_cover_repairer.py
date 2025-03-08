from typing import Any

import gurobipy as gp
from pandas import DataFrame

from src.ilp.vertex_cover_data_repair_ilp import VertexCoverDataRepairILP
from src.marginals.marginals import Marginals
from src.utils import consts
from src.utils.functional_dependency import load_fds
from src.utils.node import Node


class VertexCoverRepairer(Node):
    ILPSolution = gp.tupledict[str, gp.Var]

    def __init__(self, config: dict[str, Any]):
        super().__init__(config=config,
                         fields=["license_file_name"])

    @staticmethod
    def output_file_name() -> str:
        return consts.REPAIRED_DATA_FILE_NAME

    def node_action(self, data: DataFrame) -> DataFrame:
        fds = load_fds(self.working_dir)
        marginals = Marginals(self.working_dir)
        ilp = VertexCoverDataRepairILP(data, fds, self.config, marginals)
        ilp.solve()
        return self.__get_feasible_solution(data, ilp) if ilp.did_succeed else self.__get_feasible_solution(data, ilp)

    @staticmethod
    def __get_infeasible_solution(data: DataFrame) -> DataFrame:
        print("Model is infeasible")
        return data.drop(index=data.index)

    @staticmethod
    def __get_feasible_solution(data: DataFrame, ilp: VertexCoverDataRepairILP) -> DataFrame:
        tuples_to_remove = [i for i, x in enumerate(ilp.solution) if x == 0]
        return data.drop(index=tuples_to_remove)
