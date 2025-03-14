import os

from src.analyzers.analyzer import Analyzer
from src.storage import object_loader
from src.utils import consts
from src.utils.report import Report


class RuntimeAnalyzer(Analyzer):
    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["x_axis", "x_axis_label"]

    def analyze(self, reports: dict[str, list[Report]]) -> None:
        x_axis = self.config["x_axis"]
        runtimes = {}
        for service_name, reports in reports.items():
            runtimes[service_name] = {x_element: report.end_time - report.start_time
                                      for x_element, report in zip(x_axis[:len(reports)], reports)}

        self.save_results(runtimes, consts.RUNTIMES_RESULT_FILE_NAME)
