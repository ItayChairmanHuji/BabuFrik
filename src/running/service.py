import os
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Any

import pandas as pd
from pandas import DataFrame

from src.utils import timer
from src.utils.configuration import Configuration
from src.utils.message import Message
from src.utils.temp_dir import TempDir


@dataclass
class Service(ABC):
    working_dir: TempDir
    fds_file_path: str
    marginals_errors_margins_file_path: str
    config: Configuration
    extra_data: dict[str, Any] = None

    @property
    def name(self):
        return self.config.name

    @property
    def analyzers(self) -> list[str]:
        return self.config.analyzers

    @property
    def dynamic_fields(self) -> dict[str, list[Any]]:
        return self.config.dynamic_fields

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

    def run(self, message: Message) -> Message:
        self.working_dir.reset()
        self.extra_data = message.extra_data.copy()
        input_data = self.__load_input_data(message)
        output_data, runtime = timer.run_with_timer(lambda: self.service_action(input_data))
        self.extra_data["runtime"] = runtime
        self.extra_data["original_data_path"] = message.data_file_path
        return Message(
            from_service=self.name,
            data_file_path=self.__save_output_data(output_data),
            extra_data=self.extra_data,
            from_service_code_name=self.__module__.split(".")[-1]
        )

    @staticmethod
    def __load_input_data(message: Message) -> DataFrame:
        return pd.read_csv(message.data_file_path) if message.data_file_path is not None else None

    def __save_output_data(self, output_data: DataFrame) -> str:
        output_file_path = os.path.join(self.working_dir.path, self.output_file_name())
        output_data.to_csv(output_file_path, index=False)
        return output_file_path
