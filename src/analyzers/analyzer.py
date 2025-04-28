import os
import uuid
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Any

from pandas import DataFrame

from src.utils.configuration import Configuration
from src.utils.message import Message


@dataclass
class Analyzer(ABC):
    result_dir_path: str
    results_columns: list[str]
    config: Configuration

    @property
    def name(self):
        return self.config["analyzer_name"]

    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["x_axis", "vega_spec_name"]

    def analyze(self, dynamic_fields: dict[str, Any], message: Message) -> None:
        result_values = [
            dynamic_fields[self.config["x_axis"]] \
                if self.config["x_axis"] in dynamic_fields else message.extra_data[self.config["x_axis"]],
            self.config["x_axis_name"],
            message.extra_data["dataset_name"],
            self.y_axis_name(),
            message.from_service_code_name,
            self.analyzer_action(message)
        ]
        DataFrame({key: [value] for key, value in zip(self.results_columns, result_values)}).to_csv(
            os.path.join(self.result_dir_path, str(uuid.uuid4())), index=False)

    @abstractmethod
    def analyzer_action(self, message: Message) -> float:
        raise NotImplementedError('Analyzer must implement this method')

    @staticmethod
    @abstractmethod
    def y_axis_name() -> str:
        raise NotImplementedError('Analyzer must implement this method')

    @abstractmethod
    def title(self, message: Message) -> str:
        raise NotImplementedError('Analyzer must implement this method')
