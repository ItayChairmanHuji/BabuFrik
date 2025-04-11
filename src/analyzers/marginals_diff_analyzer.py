import pandas as pd

from src.analyzers.analyzer import Analyzer
from src.marginals.marginals import Marginals
from src.utils.message import Message


class MarginalsDiffAnalyzer(Analyzer):
    def analyzer_action(self, message: Message) -> float:
        new_data = pd.read_csv(message.data_file_path)
        original_data = pd.read_csv(message.extra_data["original_data_path"])
        return Marginals(original_data).mean_distance(Marginals(new_data))

    @staticmethod
    def y_axis_name() -> str:
        return "marginals_difference"

    def title(self, message: Message) -> str:
        return f"{message.from_service_code_name} marginals difference"
