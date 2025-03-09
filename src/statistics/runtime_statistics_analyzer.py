from src.statistics.statistics_analayzer import StatisticsAnalyzer
from src.utils.report import Report


class RuntimeStatisticsAnalyzer(StatisticsAnalyzer):
    def analyze_statistics(self, reports: dict[str, list[Report]]) -> None:
        for service_name, reports in reports.items():
            runtimes = [report.end_time - report.start_time for report in reports]
            self.plot(runtimes, service_name, plot_topic="runtime")
