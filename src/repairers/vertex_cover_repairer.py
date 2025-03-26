import gurobipy as gp
import pandas as pd
from pandas import DataFrame

from src.ilp.vertex_cover_data_repair_ilp import VertexCoverDataRepairILP
from src.marginals.marginals import Marginals
from src.running.service import Service
from src.storage import object_loader
from src.utils import consts
from src.violations.functional_dependency import load_fds, FunctionalDependency, is_common_left_hand_side


class VertexCoverRepairer(Service):
    ILPSolution = gp.tupledict[str, gp.Var]

    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["license_file_name"]

    @staticmethod
    def output_file_name() -> str:
        return consts.REPAIRED_DATA_FILE_NAME

    def service_action(self, data: DataFrame) -> DataFrame:
        fds = load_fds(self.fds_file_path)
        marginals: Marginals = object_loader.load(self.extra_data["marginals_file_path"])
        return self.__repair_data(data, fds, marginals) if not is_common_left_hand_side(fds) else (
            pd.concat([self.__repair_data(block.reset_index().drop(columns=["index"]), fds, marginals)
                       for _, block in data.groupby(fds[0].source)]))

    @staticmethod
    def __get_infeasible_solution(data: DataFrame) -> DataFrame:
        print("Model is infeasible")
        return data.drop(index=data.index)

    @staticmethod
    def __get_feasible_solution(data: DataFrame, ilp: VertexCoverDataRepairILP) -> DataFrame:
        tuples_to_remove = [i for i, x in enumerate(ilp.solution) if x == 0]
        return data.drop(index=tuples_to_remove)

    def __repair_data(self, data: DataFrame, fds: list[FunctionalDependency], marginals: Marginals) -> DataFrame:
        ilp = VertexCoverDataRepairILP(data, fds, self.config, marginals)
        ilp.solve()
        return self.__get_feasible_solution(data, ilp) if ilp.did_succeed else self.__get_feasible_solution(data, ilp)
