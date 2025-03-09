import os
import time
from abc import abstractmethod, ABC
from dataclasses import dataclass

from pandas import DataFrame

from src.storage.temp_data import TempData
from src.utils.configuration import Configuration
from src.utils.report import Report


@dataclass
class Service(ABC):
    working_dir: str
    fds_file_path: str
    marginals_errors_margins_file_path: str
    config: Configuration

    @property
    def name(self):
        return self.__module__.split(".")[-1]

    @staticmethod
    @abstractmethod
    def output_file_name() -> str:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def mandatory_fields() -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def service_action(self, data: DataFrame) -> DataFrame:
        raise NotImplementedError()

    def run(self, input_file_path: str) -> Report:
        output_file_path = os.path.join(self.working_dir, self.output_file_name())
        with TempData(input_file_path, output_file_path) as temp_data:
            start_time = time.time()
            temp_data.data = self.service_action(temp_data.data)
            end_time = time.time()
        return Report(service_name=self.name,
                      start_time=start_time,
                      end_time=end_time,
                      output_file_path=output_file_path)
