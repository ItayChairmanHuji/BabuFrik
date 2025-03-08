import pandas as pd

from src.statistics.statistics_analayzer import StatisticsAnalyzer
from src.utils.functional_dependency import FunctionalDependency
from src.utils.node_report import NodeReport
from src.utils.violations_checker import ViolationsChecker


class ViolationsNumberStatisticsAnalyzer(StatisticsAnalyzer):
    def __init__(self, working_dir: str, x_axis: list[float], x_axis_name: str,
                 fds: list[FunctionalDependency], nodes_type_to_check: str = "synthesizer"):
        super().__init__(working_dir, x_axis, x_axis_name)
        self.fds = fds
        self.nodes_type_to_check = nodes_type_to_check

    def analyze_statistics(self, nodes_reports: dict[str, list[NodeReport]]) -> None:
        for node_name, reports in nodes_reports.items():
            if node_name.split('_')[-1] != self.nodes_type_to_check:
                continue

            violations = [self.__calc_total_num_of_violations(report) for report in reports]
            self.plot(violations, node_name, plot_topic="violations")

    def __calc_total_num_of_violations(self, report: NodeReport):
        data = pd.read_csv(report.output_file_path)
        return sum(ViolationsChecker.count_functional_dependency_violations(data, fd) for fd in self.fds)
