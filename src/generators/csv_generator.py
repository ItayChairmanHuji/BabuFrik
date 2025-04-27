import os

import pandas as pd
from pandas import DataFrame

from src.running.service import Service
from src.utils import consts


class CSVGenerator(Service):
    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["file_name"]

    @staticmethod
    def output_file_name() -> str:
        return consts.GENERATED_DATA_FILE_NAME

    def service_action(self, _) -> DataFrame:
        file_path = os.path.join(consts.RESOURCES_DIR_PATH, self.config["file_name"])
        self.extra_data["dataset_name"] = self.config["file_name"].split(".")[0]
        return pd.read_csv(str(file_path))
