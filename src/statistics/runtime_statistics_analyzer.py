from src.statistics.statistics_analayzer import StatisticsAnalyzer
from src.utils.node_report import NodeReport


class RuntimeStatisticsAnalyzer(StatisticsAnalyzer):
    def analyze_statistics(self, nodes_reports: dict[str, list[NodeReport]]) -> None:
        for node_name, reports in nodes_reports.items():
            runtimes = [report.end_time - report.start_time for report in reports]
            self.plot(runtimes, node_name, plot_topic="runtime")
