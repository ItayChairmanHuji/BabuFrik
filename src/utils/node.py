import os
import time
from abc import abstractmethod, ABC
from typing import Any

from pandas import DataFrame

from src.utils.node_report import NodeReport
from src.utils.temp_data import TempData


class Node(ABC):
    def __init__(self, config: dict[str, Any], fields: list[str] = None):
        self.config = config
        self.working_dir = ""

        fields_to_check = fields if fields is not None else []
        self.__validate_config(config, fields_to_check)

    def __validate_config(self, config: dict[str, Any], fields: list[str]) -> None:
        missing_fields = [field for field in fields if field not in config]
        if len(missing_fields) > 0:
            raise ValueError(
                f"The following fields are missing: {missing_fields} from the config of the node: {self.name}")

    @property
    def name(self):
        return self.__module__.split(".")[-1]

    @staticmethod
    @abstractmethod
    def output_file_name():
        raise NotImplementedError()

    def run(self, working_dir: str, input_file_path: str) -> NodeReport:
        self.working_dir = working_dir
        output_file_path = os.path.join(self.working_dir, self.output_file_name())
        with TempData(input_file_path, output_file_path) as temp_data:
            start_time = time.time()
            temp_data.data = self.node_action(temp_data.data)
            end_time = time.time()
        return NodeReport(node_name=self.name,
                          start_time=start_time,
                          end_time=end_time,
                          output_file_path=output_file_path)

    @abstractmethod
    def node_action(self, data: DataFrame) -> DataFrame:
        raise NotImplementedError()
