from src.analyzers.analyzer import Analyzer
from src.utils.message import Message


class RuntimeAnalyzer(Analyzer):
    def analyzer_action(self, message: Message) -> float:
        return float(message.extra_data["runtime"])

    @staticmethod
    def y_axis_name() -> str:
        return "runtime"

    def title(self, message: Message) -> str:
        return f"{message.from_service_code_name} runtime"