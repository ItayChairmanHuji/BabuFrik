import os.path
from abc import ABC, abstractmethod

import pandas as pd
from pandas import DataFrame

from src.ilp.optimal_data_repair_ilp import OptimalDataRepairILP
from src.marginals.marginals import Marginals
from src.running.service import Service
from src.storage import object_loader
from src.utils import consts
from src.violations.functional_dependency import load_fds, FunctionalDependency, is_common_left_hand_side


class ILPRepairerBase(Service, ABC):
    @property
    @abstractmethod
    def ilp_type(self) -> type[OptimalDataRepairILP]:
        raise NotImplementedError("ILP type not implemented")

    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["license_file_name"]

    @staticmethod
    def output_file_name() -> str:
        return consts.REPAIRED_DATA_FILE_NAME

    def service_action(self, data: DataFrame) -> DataFrame:
        fds = load_fds(self.fds_file_path)
        marginals: Marginals = self.__load_marginals()
        self.config["marginals_error_margins_file_path"] = self.marginals_errors_margins_file_path
        return self.__repair_data(data, fds, marginals) if \
            not is_common_left_hand_side(fds) else self.__common_left_hand_side_optimization(data,fds, marginals)

    def __load_marginals(self) -> Marginals:
        return object_loader.load(self.extra_data["marginals_file_path"]) \
            if "marginals_file_path" in self.extra_data \
            else object_loader.load(str(os.path.join(consts.RESOURCES_DIR_PATH, consts.MARGINALS_FILE_NAME)))

    @staticmethod
    def __get_infeasible_solution(data: DataFrame) -> DataFrame:
        print("Model is infeasible")
        return data.drop(index=data.index)

    @staticmethod
    def __get_feasible_solution(data: DataFrame, ilp: OptimalDataRepairILP) -> DataFrame:
        tuples_to_remove = [index for index, value in ilp.solution.items() if value == 0]
        print(f"Removing {len(tuples_to_remove)} tuples")
        return data.drop(index=tuples_to_remove)

    def __repair_data(self, data: DataFrame, fds: list[FunctionalDependency], marginals: Marginals) -> DataFrame:
        ilp = self.ilp_type(data, fds, self.config, marginals)
        ilp.solve()
        return self.__get_feasible_solution(data, ilp) if ilp.did_succeed else self.__get_infeasible_solution(data)

    def __common_left_hand_side_optimization(self, data: DataFrame,
                                             fds: list[FunctionalDependency], marginals: Marginals) -> DataFrame:
        print(data.groupby(fds[0].lhs).size())
        return pd.concat([self.__repair_data(block.reset_index().drop(columns=["index"]), fds, marginals) for _, block in data.groupby(fds[0].lhs)])