import os
from abc import abstractmethod, ABC
from typing import Any

from pandas import DataFrame

from src.utils.temp_data import TempData


class Node(ABC):
    def __init__(self, config: dict[str, Any], fields: list[str] = None):
        self.config = config
        self.working_dir = ""
        fields_to_check = fields if fields is not None else []
        self.validate_config(config, fields_to_check)

    def validate_config(self, config: dict[str, Any], fields: list[str]) -> None:
        missing_fields = [field for field in fields if field not in config]
        if len(missing_fields) > 0:
            raise ValueError(
                f"The following fields are missing: {missing_fields} from the config of the node: {self.name}")

    def name(self):
        return self.__module__.split(".")[-1]

    @staticmethod
    @abstractmethod
    def output_file_path():
        raise NotImplementedError()

    def run(self, working_dir:str, input_file_path: str):
        self.working_dir = working_dir
        input_file_path = os.path.join(self.working_dir, input_file_path) if input_file_path is not None else None
        output_file_path = os.path.join(self.working_dir, self.output_file_path())
        with TempData(input_file_path, output_file_path) as temp_data:
            temp_data.data = self.node_action(temp_data.data)

    @abstractmethod
    def node_action(self, data: DataFrame) -> DataFrame:
        raise NotImplementedError()
