import json
import os
from itertools import chain
from typing import Any

import numpy as np
from pandas import DataFrame
from src.utils.functional_dependency import FunctionalDependency

from src.utils import consts
from src.utils.node import Node


class ColumnsCleaner(Node):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config=config,
                         fields=["max_columns_to_keep"])

    @staticmethod
    def output_file_path() -> str:
        return consts.CLEANED_DATA_FILE_NAME

    def node_action(self, data: DataFrame) -> DataFrame:
        max_columns_to_keep = self.config["max_columns_to_keep"]
        fds = self.__load_fds()
        fds_columns = set(chain.from_iterable((fd.source, fd.target) for fd in fds))
        num_of_columns_to_remove = max(len(data.columns) - len(fds_columns) - max_columns_to_keep, 0)
        columns_to_remove = (np.random
                             .choice(data.columns.difference(fds_columns), num_of_columns_to_remove, replace=False))
        return data.drop(columns=columns_to_remove)

    def __load_fds(self) -> list[FunctionalDependency]:
        fd_file = os.path.join(self.working_dir, consts.FUNCTIONAL_DEPENDENCIES_FILE_NAME)
        fds_as_json = json.load(open(fd_file))
        return [FunctionalDependency(source=fd["source"], target=fd["target"]) for fd in fds_as_json]
