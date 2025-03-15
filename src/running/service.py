import os
import time
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Any

import pandas as pd
from pandas import DataFrame

from src.utils import consts
from src.utils.configuration import Configuration
from src.utils.message import Message
from src.utils.report import Report


@dataclass
class Service(ABC):
    working_dir: str
    fds_file_path: str
    marginals_errors_margins_file_path: str
    config: Configuration

    @property
    def name(self):
        return self.config["config_name"]

    @property
    def analyzers(self) -> list[str]:
        return self.config["analyzers"]

    @property
    def dynamic_fields(self) -> dict[str, list[Any]]:
        return self.config["dynamic_fields"]

    @staticmethod
    @abstractmethod
    def mandatory_fields() -> list[str]:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def output_file_name() -> str:
        raise NotImplementedError()

    @abstractmethod
    def service_action(self, data: DataFrame) -> DataFrame:
        raise NotImplementedError()

    def __get_shared_file_path(self, file_name: str) -> str:
        dir_path = self.working_dir
        while not os.path.exists(file_path := os.path.join(dir_path, file_name)):
            if dir_path == consts.ROOT_DIR:
                raise FileNotFoundError(f"The file with name {file_name} does not exist")
            dir_path = os.path.dirname(dir_path)
        return file_path

    def run(self, message: Message) -> Report:
        self.__update_working_dir(message)
        input_data = self.__load_input_data(message)
        start_time = time.time()
        output_data = self.service_action(input_data)
        end_time = time.time()
        output_file_path = self.__save_output_data(output_data)
        return Report(service=self,
                      start_time=start_time,
                      end_time=end_time,
                      output_file_path=output_file_path)

    def __update_working_dir(self, message: Message) -> None:
        self.working_dir = message.working_dir
        os.makedirs(self.working_dir, exist_ok=True)

    @staticmethod
    def __load_input_data(message: Message) -> DataFrame:
        return pd.read_csv(message.input_file) if message.input_file is not None else None

    def __save_output_data(self, output_data: DataFrame) -> str:
        output_file_path = os.path.join(self.working_dir, self.output_file_name())
        output_data.to_csv(output_file_path, index=False)
