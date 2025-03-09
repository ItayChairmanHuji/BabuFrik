import os
from abc import abstractmethod, ABC
from dataclasses import dataclass

from matplotlib.figure import Figure

from src.utils.configuration import Configuration
from src.utils.node_report import NodeReport


@dataclass
class StatisticsAnalyzer(ABC):
    working_dir: str
    figure: Figure
    config: Configuration

    @staticmethod
    def mandatory_fields() -> list[str]:
        raise ["x_axis", "x_axis_label"]

    @abstractmethod
    def analyze_statistics(self, reports: list[NodeReport]) -> None:
        raise NotImplementedError()

    def plot(self, data: list[float], node_name: str, plot_topic: str) -> None:
        self.figure.clear()
        x_axis = self.config["x_axis"]
        x_axis_label = self.config["x_axis_label"]
        ax = self.figure.subplots(nrows=1, ncols=1)
        ax.plot(x_axis[:len(data)], data)
        ax.set_title(f"{node_name} num of {plot_topic} as function of size")
        ax.set_xlabel(x_axis_label)
        ax.set_ylabel(plot_topic)
        figure_path = os.path.join(self.working_dir, f"{node_name}_{plot_topic}.png")
        self.figure.savefig(figure_path)
