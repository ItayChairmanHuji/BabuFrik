import os
from typing import Any

import pandas as pd
from pandas import DataFrame

from src.utils import consts
from src.utils.node import Node


class CSVGenerator(Node):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config=config,
                         fields=["file_name"])

    @staticmethod
    def output_file_path() -> str:
        return consts.GENERATED_DATA_FILE_NAME

    def node_action(self, _) -> DataFrame:
        file_path = os.path.join(consts.RESOURCES_DIR_PATH, self.config["file_name"])
        return pd.read_csv(str(file_path))
