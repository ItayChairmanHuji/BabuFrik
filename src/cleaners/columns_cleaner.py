from itertools import chain

import numpy as np
from pandas import DataFrame

from src.running.service import Service
from src.utils import consts
from src.violations import functional_dependency


class ColumnsCleaner(Service):
    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["max_columns_to_keep"]

    @staticmethod
    def output_file_name() -> str:
        return consts.CLEANED_DATA_FILE_NAME

    def service_action(self, data: DataFrame) -> DataFrame:
        max_columns_to_keep = self.config["max_columns_to_keep"]
        fds = functional_dependency.load_fds(self.fds_file_path)
        fds_columns = set(chain.from_iterable((fd.source, fd.target) for fd in fds))
        num_of_columns_to_remove = max(len(data.columns) - len(fds_columns) - max_columns_to_keep, 0)
        columns_to_remove = (np.random
                             .choice(data.columns.difference(fds_columns), num_of_columns_to_remove, replace=False))
        return data.drop(columns=columns_to_remove)
