from typing import Any

import numpy as np
from pandas import DataFrame

from src.utils import consts
from src.utils.node import Node


class SizeLimitCleaner(Node):

    def __init__(self, config: dict[str, Any]):
        super().__init__(config=config,
                         fields=["size_limit"])

    @staticmethod
    def output_file_path() -> str:
        return consts.CLEANED_DATA_FILE_NAME

    def node_action(self, data: DataFrame) -> DataFrame:
        num_of_rows_to_remove = max(len(data) - self.config["size_limit"], 0)
        rows_to_remove = np.random.choice(data.index, num_of_rows_to_remove, replace=False)
        return data.drop(index=rows_to_remove)
