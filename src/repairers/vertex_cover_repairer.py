import os

import gurobipy as gp
from pandas import DataFrame

from src.ilp.vertex_cover_data_repair_ilp import VertexCoverDataRepairILP
from src.marginals.marginals import Marginals
from src.running.service import Service
from src.storage import object_loader
from src.utils import consts
from src.violations.functional_dependency import load_fds


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
        marginals: Marginals = object_loader.load(self.__get_marginals_file_path())
        ilp = VertexCoverDataRepairILP(data, fds, self.config, marginals)
        ilp.solve()
        return self.__get_feasible_solution(data, ilp) if ilp.did_succeed else self.__get_feasible_solution(data, ilp)

    def __get_marginals_file_path(self) -> str:
        local_marginals_file = str(os.path.join(self.working_dir, consts.MARGINALS_FILE_NAME))
        external_marginals_file = str(os.path.join(consts.RESOURCES_DIR_PATH, consts.MARGINALS_FILE_NAME))
        return local_marginals_file if os.path.exists(local_marginals_file) else external_marginals_file

    @staticmethod
    def __get_infeasible_solution(data: DataFrame) -> DataFrame:
        print("Model is infeasible")
        return data.drop(index=data.index)

    @staticmethod
    def __get_feasible_solution(data: DataFrame, ilp: VertexCoverDataRepairILP) -> DataFrame:
        tuples_to_remove = [i for i, x in enumerate(ilp.solution) if x == 0]
        return data.drop(index=tuples_to_remove)
