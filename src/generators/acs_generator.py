from typing import Any

from folktables import ACSDataSource
from pandas import DataFrame

from src.utils import consts
from src.utils.node import Node


class ACSGenerator(Node):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config=config,
                         fields=["survey_year", "horizon", "survey", "states"])

    @staticmethod
    def name():
        return "acs_generator"

    @staticmethod
    def output_file_path() -> str:
        return consts.GENERATED_DATA_FILE_NAME

    def node_action(self, _) -> DataFrame:
        return ACSDataSource(survey_year=self.config["survey_year"],
                             horizon=self.config["horizon"],
                             survey=self.config["survey"]).get_data(states=self.config["states"], download=True)
