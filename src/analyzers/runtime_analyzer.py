from src.analyzers.analyzer import Analyzer
from src.utils.report import Report


class RuntimeAnalyzer(Analyzer):
    def analyzer_action(self, report: Report) -> float:
        return report.end_time - report.start_time
