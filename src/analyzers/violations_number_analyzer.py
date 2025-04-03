import pandas as pd

from src.analyzers.analyzer import Analyzer
from src.utils.message import Message
from src.violations import violations_checker
from src.violations.functional_dependency import load_fds


class ViolationsNumberAnalyzer(Analyzer):
    def analyzer_action(self, message: Message) -> float:
        data = pd.read_csv(message.data_file_path)
        fds = load_fds(message.extra_data["fds_file_path"])
        return sum(violations_checker.count_functional_dependency_violations(data, fd) for fd in fds)

    @staticmethod
    def y_axis_name() -> str:
        return "violations"

    def title(self, message: Message) -> str:
        return f"{message.from_service_code_name} violations count"