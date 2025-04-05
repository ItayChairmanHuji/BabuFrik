from pandas import DataFrame

from src.running.service import Service
from src.utils import consts


class GreedyRepairer(Service):
    @staticmethod
    def mandatory_fields() -> list[str]:
        return []

    @staticmethod
    def output_file_name() -> str:
        return consts.REPAIRED_DATA_FILE_NAME

    def service_action(self, data: DataFrame) -> DataFrame:
        