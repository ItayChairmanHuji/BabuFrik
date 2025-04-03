from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Any

from matplotlib import pyplot as plt
from matplotlib.figure import Figure
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
        self.run.log({self.title(message): self.plot()})

    def plot(self) -> Figure:
        figure, axes = plt.subplots()
        x_axis, y_axis = zip(*self.table.data)
        axes.plot(x_axis, y_axis, linewidth=3)
        axes.set_xlabel(self.table.columns[0])
        axes.set_ylabel(self.table.columns[1])
        return figure

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
