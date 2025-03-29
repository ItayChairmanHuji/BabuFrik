from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Any

import wandb
from wandb import Table
from wandb.apis.public import Run

from src.utils.configuration import Configuration
from src.utils.message import Message


@dataclass
class Analyzer(ABC):
    run: Run
    table: Table
    config: Configuration

    @property
    def name(self):
        return self.config["analyzer_name"]

    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["x_axis", "vega_spec_name"]

    def analyze(self, dynamic_fields: dict[str, Any], message: Message) -> None:
        x_axis_value = dynamic_fields[self.config["x_axis"]] \
            if self.config["x_axis"] in dynamic_fields else message.extra_data[self.config["x_axis"]]
        self.table.add_data(x_axis_value, self.analyzer_action(message))
        service_name = message.from_service
        self.run.log({f"{self.section()}/{service_name}": wandb.plot.plot_table(
            data_table=wandb.Table(columns=self.table.columns, data=self.table.data),
            vega_spec_name=self.config["vega_spec_name"],
            fields={"x": self.table.columns[0], "y": self.table.columns[1], "stroke": None},
            string_fields={"title": service_name},
            split_table=True,
        )})

    @abstractmethod
    def analyzer_action(self, message: Message) -> float:
        raise NotImplementedError('Analyzer must implement this method')

    @staticmethod
    @abstractmethod
    def section() -> str:
        raise NotImplementedError('Analyzer must implement this method')
