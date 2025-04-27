from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Any

from pandas import DataFrame

from src.utils.configuration import Configuration
from src.utils.message import Message


@dataclass
class Analyzer(ABC):
    result_file_path: str
    results: DataFrame
    config: Configuration

    @property
    def name(self):
        return self.config["analyzer_name"]

    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["x_axis", "vega_spec_name"]

    def analyze(self, dynamic_fields: dict[str, Any], message: Message) -> None:
        self.results.loc[len(self.results)] = [
            dynamic_fields[self.config["x_axis"]] \
                if self.config["x_axis"] in dynamic_fields else message.extra_data[self.config["x_axis"]],
            self.config["x_axis_name"],
            message.extra_data["dataset_name"],
            self.y_axis_name(),
            message.from_service_code_name,
            self.analyzer_action(message)
        ]
        self.results.to_csv(self.result_file_path, index=False)

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
