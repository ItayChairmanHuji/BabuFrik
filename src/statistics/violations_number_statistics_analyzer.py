import pandas as pd

from src.statistics.statistics_analayzer import StatisticsAnalyzer
from src.utils.functional_dependency import FunctionalDependency, load_fds
from src.utils.node_report import NodeReport
from src.utils.violations_checker import ViolationsChecker


class ViolationsNumberStatisticsAnalyzer(StatisticsAnalyzer):
    def __init__(self, working_dir: str, x_axis: list[float], x_axis_name: str, nodes_type: str = "synthesizer"):
        super().__init__(working_dir, x_axis, x_axis_name)
        self.nodes_type = nodes_type

    def analyze_statistics(self, nodes_reports: dict[str, list[NodeReport]]) -> None:
        fds = load_fds(self.working_dir)
        for node_name, reports in nodes_reports.items():
            if node_name.split('_')[-1] != self.nodes_type:
                continue

            violations = [self.__calc_total_num_of_violations(report, fds) for report in reports]
            self.plot(violations, node_name, plot_topic="violations")

    @staticmethod
    def __calc_total_num_of_violations(report: NodeReport, fds: list[FunctionalDependency]):
        data = pd.read_csv(report.output_file_path)
        return sum(ViolationsChecker.count_functional_dependency_violations(data, fd) for fd in fds)
