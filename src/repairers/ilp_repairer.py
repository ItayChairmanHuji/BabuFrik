import gurobipy as gp
from pandas import DataFrame

from src.ilp.synthetic_data_repair_ilp import SyntheticDataRepairILP
from src.marginals.marginals import Marginals
from src.running.service import Service
from src.storage import object_loader
from src.utils import consts
from src.violations.functional_dependency import load_fds


class ILPRepairer(Service):
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
        self.config["marginals_error_margins_file_path"] = self.marginals_errors_margins_file_path
        ilp = SyntheticDataRepairILP(data, fds, self.config, marginals)
        ilp.solve()
        return self.__get_feasible_solution(data, ilp) if ilp.did_succeed else self.__get_feasible_solution(data, ilp)

    @staticmethod
    def __get_infeasible_solution(data: DataFrame) -> DataFrame:
        print("Model is infeasible")
        return data.drop(index=data.index)

    @staticmethod
    def __get_feasible_solution(data: DataFrame, ilp: SyntheticDataRepairILP) -> DataFrame:
        tuples_to_remove = [i for i, x in enumerate(ilp.solution) if x == 0]
        return data.drop(index=tuples_to_remove)
