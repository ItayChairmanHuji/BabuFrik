import pandas as pd

from src.analyzers.analyzer import Analyzer
from src.utils.message import Message


class RepairSizeAnalyzer(Analyzer):
    def analyzer_action(self, message: Message) -> float:
        repaired_data = pd.read_csv(message.data_file_path)
        original_data = pd.read_csv(message.extra_data["original_data_path"])
        return len(original_data) - len(repaired_data)

    @staticmethod
    def y_axis_name() -> str:
        return "repair_size"

    def title(self, message: Message) -> str:
        return f"{message.from_service_code_name} repair size"
