import pandas as pd
from pandas import DataFrame

from src.runner.service import Service
from src.utils import consts


class AdultGenerator(Service):

    @staticmethod
    def mandatory_fields() -> list[str]:
        return []

    @staticmethod
    def output_file_name() -> str:
        return consts.GENERATED_DATA_FILE_NAME

    def service_action(self, _) -> DataFrame:
        url = "https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data"
        names = ['age', 'workclass', 'fnlwgt', 'education', 'education-num', 'marital-status', 'occupation',
                 'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week', 'native-country',
                 'income']
        return pd.read_csv(url, names=names)
