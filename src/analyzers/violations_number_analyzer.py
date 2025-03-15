import pandas as pd

from src.analyzers.analyzer import Analyzer
from src.utils.report import Report
from src.violations import violations_checker
from src.violations.functional_dependency import load_fds


class ViolationsNumberAnalyzer(Analyzer):
    def analyzer_action(self, report: Report) -> float:
        data = pd.read_csv(report.output_file_path)
        fds = load_fds(report.service.fds_file_path)
        return sum(violations_checker.count_functional_dependency_violations(data, fd) for fd in fds)
