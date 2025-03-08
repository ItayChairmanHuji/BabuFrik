from typing import Any

import pandas as pd
from pandas import DataFrame

from src.utils import consts
from src.utils.node import Node


class AdultGenerator(Node):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config=config,
                         fields=[])

    @staticmethod
    def output_file_name() -> str:
        return consts.GENERATED_DATA_FILE_NAME

    def node_action(self, _) -> DataFrame:
        url = "https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data"
        names = ['age', 'workclass', 'fnlwgt', 'education', 'education-num', 'marital-status', 'occupation',
                 'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week', 'native-country',
                 'income']
        return pd.read_csv(url, names=names)
