import numpy as np
from pandas import DataFrame

from src.running.service import Service
from src.utils import consts


class SizeLimitCleaner(Service):

    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["size_limit"]

    @staticmethod
    def output_file_name() -> str:
        return consts.CLEANED_DATA_FILE_NAME

    def service_action(self, data: DataFrame) -> DataFrame:
        num_of_rows_to_remove = max(len(data) - self.config["size_limit"], 0)
        rows_to_remove = np.random.choice(data.index, num_of_rows_to_remove, replace=False)
        return data.drop(index=rows_to_remove)
