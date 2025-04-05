import os

import numpy as np
from pandas import DataFrame

from src.marginals.marginals import Marginals
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.repairers import optimal_repair
from src.running.service import Service
from src.storage import object_loader
from src.utils import consts
from src.violations.functional_dependency import load_fds


class GreedyRepairer(Service):
    @staticmethod
    def mandatory_fields() -> list[str]:
        return []

    @staticmethod
    def output_file_name() -> str:
        return consts.REPAIRED_DATA_FILE_NAME

    def service_action(self, data: DataFrame) -> DataFrame:
        fds = load_fds(self.fds_file_path)
        marginals: Marginals = self.__load_marginals()
        marginals_errors_margins = MarginalsErrorsMargins(self.config["marginals_error_margins_file_path"])
        repaired_data = optimal_repair.repair_data(data, fds)
        while self.__should_remove_tuple(repaired_data, marginals, marginals_errors_margins):
            tuple_to_remove = self.__find_tuple_to_remove(repaired_data, marginals)
            repaired_data.drop(index=tuple_to_remove)
        return repaired_data

    def __load_marginals(self) -> Marginals:
        return object_loader.load(self.extra_data["marginals_file_path"]) \
            if "marginals_file_path" in self.extra_data \
            else object_loader.load(str(os.path.join(consts.RESOURCES_DIR_PATH, consts.MARGINALS_FILE_NAME)))

    @staticmethod
    def __should_remove_tuple(data: DataFrame, marginals: Marginals,
                              marginals_errors_margins: MarginalsErrorsMargins) -> bool:
        return any(distance > marginals_errors_margins[attrs]
                   for attrs, distance in marginals.distance(Marginals(data)).iteritems())

    def __find_tuple_to_remove(self, data: DataFrame, marginals: Marginals) -> int:
        return int(np.argmin(self.__calculate_tuple_cost(data, ind, marginals) for ind in range(len(data))))

    @staticmethod
    def __calculate_tuple_cost(data: DataFrame, tuple_index: int, marginals: Marginals) -> float:
        data_without_tuple = data.drop(index=tuple_index)
        marginals_without_tuple = Marginals(data_without_tuple)
        return marginals.mean_distance(marginals_without_tuple)
