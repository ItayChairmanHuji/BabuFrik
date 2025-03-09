import pandas as pd

from src.statistics.statistics_analayzer import StatisticsAnalyzer
from src.utils.functional_dependency import FunctionalDependency, load_fds
from src.utils.report import Report
from src.utils.violations_checker import ViolationsChecker


class ViolationsNumberStatisticsAnalyzer(StatisticsAnalyzer):
    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["x_axis", "x_axis_label", "services_type"]

    def analyze_statistics(self, reports: dict[str, list[Report]]) -> None:
        fds = load_fds(self.working_dir)
        for service_name, reports in reports.items():
            if service_name.split('_')[-1] != self.config["services_type"]:
                continue

            violations = [self.__calc_total_num_of_violations(report, fds) for report in reports]
            self.plot(violations, service_name, plot_topic="violations")

    @staticmethod
    def __calc_total_num_of_violations(report: Report, fds: list[FunctionalDependency]):
        data = pd.read_csv(report.output_file_path)
        return sum(ViolationsChecker.count_functional_dependency_violations(data, fd) for fd in fds)
