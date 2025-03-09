import os.path

from folktables import ACSDataSource
from pandas import DataFrame

from src.running.service import Service
from src.utils import consts


class ACSGenerator(Service):
    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["survey_year", "horizon", "survey", "states"]

    @staticmethod
    def output_file_name() -> str:
        return consts.GENERATED_DATA_FILE_NAME

    def service_action(self, _) -> DataFrame:
        return (ACSDataSource(survey_year=self.config["survey_year"],
                              horizon=self.config["horizon"],
                              survey=self.config["survey"],
                              root_dir=os.path.join(consts.RESOURCES_DIR_PATH, "data"))
                .get_data(states=self.config["states"], download=True))
