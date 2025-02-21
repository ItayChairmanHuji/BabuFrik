from typing import Any

from pandas import DataFrame

from src.utils import consts
from src.utils.node import Node


class NanRemovalCleaner(Node):

    def __init__(self, config: dict[str, Any]):
        super().__init__(config=config,
                         fields=["empty_values_threshold"])

    @staticmethod
    def output_file_path() -> str:
        return consts.CLEANED_DATA_FILE_NAME

    def node_action(self, data: DataFrame) -> DataFrame:
        columns_with_empty_values = self.get_columns_with_empty_values(data)
        columns_to_remove = self.get_columns_to_remove(data, columns_with_empty_values)
        columns_to_remove_data = [column for column in columns_with_empty_values if column not in columns_to_remove]
        return data.drop(columns=columns_to_remove).dropna(subset=columns_to_remove_data)

    @staticmethod
    def get_columns_with_empty_values(data: DataFrame) -> list[str]:
        return data.columns[data.isna().any()]

    def get_columns_to_remove(self, data: DataFrame, columns_with_empty_values: list[str]) -> list[str]:
        return [column for column in columns_with_empty_values if
                self.get_empty_values_rate_in_column(data, column) >= self.config["empty_values_threshold"]]

    @staticmethod
    def get_empty_values_rate_in_column(data: DataFrame, column: str) -> float:
        num_of_empty_values = data[column].isna().sum()
        total_num_of_values = len(data[column])
        return num_of_empty_values / total_num_of_values
