import os
from abc import abstractmethod, ABC

from matplotlib import pyplot as plt

from src.utils.node_report import NodeReport


class StatisticsAnalyzer(ABC):
    def __init__(self, working_dir: str, x_axis: list[float], x_axis_name: str):
        self.working_dir = working_dir
        self.figure = plt.figure()
        self.x_axis = x_axis
        self.x_axis_name = x_axis_name

    @abstractmethod
    def analyze_statistics(self, reports: list[NodeReport]) -> None:
        raise NotImplementedError()

    def plot(self, data: list[float], node_name: str, plot_topic: str) -> None:
        self.figure.clear()
        ax = self.figure.subplots(nrows=1, ncols=1)
        ax.plot(self.x_axis[:len(data)], data)
        ax.set_title(f"{node_name} num of {plot_topic} as function of size")
        ax.set_xlabel(self.x_axis_name)
        ax.set_ylabel(plot_topic)
        figure_path = os.path.join(self.working_dir, f"{node_name}_{plot_topic}_graph.png")
        self.figure.savefig(figure_path)
