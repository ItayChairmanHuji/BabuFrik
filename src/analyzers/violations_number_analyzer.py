import pandas as pd

from src.analyzers.analyzer import Analyzer
from src.utils import consts
from src.utils.report import Report
from src.violations import violations_checker
from src.violations.functional_dependency import FunctionalDependency, load_fds


class ViolationsNumberAnalyzer(Analyzer):
    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["x_axis", "x_axis_label", "services_types"]

    def analyze(self, reports: dict[str, list[Report]]) -> None:
        fds = load_fds(self.fds_file_path)
        x_axis = self.config["x_axis"]
        x_label = self.config["x_axis_label"]
        violations = {}
        for service_name, reports in reports.items():
            if service_name.split('_')[-1] not in self.config["services_types"]:
                continue
                
            violations[f"{service_name}_runtime"] = [
                {x_label: x_element, "violations": self.__calc_total_num_of_violations(report, fds)}
                for x_element, report in zip(x_axis[:len(reports)], reports)]

        self.save_results(violations, consts.VIOLATIONS_RESULT_FILE_NAME)

    @staticmethod
    def __calc_total_num_of_violations(report: Report, fds: list[FunctionalDependency]):
        data = pd.read_csv(report.output_file_path)
        return sum(violations_checker.count_functional_dependency_violations(data, fd) for fd in fds)
