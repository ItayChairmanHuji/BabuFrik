from src.analyzers.analyzer import Analyzer
from src.utils.message import Message


class RuntimeAnalyzer(Analyzer):
    def analyzer_action(self, message: Message) -> float:
        return float(message.extra_data["runtime"])

    @staticmethod
    def section() -> str:
        return "runtime"
