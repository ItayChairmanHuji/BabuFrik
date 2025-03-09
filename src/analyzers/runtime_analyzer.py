from src.analyzers.analyzer import Analyzer
from src.utils.report import Report


class RuntimeAnalyzer(Analyzer):
    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["x_axis", "x_axis_label"]

    def analyze(self, reports: dict[str, list[Report]]) -> None:
        for service_name, reports in reports.items():
            runtimes = [report.end_time - report.start_time for report in reports]
            self.plot(runtimes, service_name, plot_topic="runtime")
