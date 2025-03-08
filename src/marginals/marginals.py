import itertools
import os
from typing import Union, Any

from pandas import DataFrame, Series

from src.utils import consts
from src.utils.multi_index_series import MultiIndexSeries
from src.utils.object_loader import ObjectLoader


class Marginals:
    def __init__(self, data: Union[DataFrame, str]):
        if not isinstance(data, DataFrame) and not isinstance(data, str):
            raise TypeError("data must be a DataFrame or a string")

        self.marginals = self.__calc_marginals(data) if isinstance(data, DataFrame) else self.__load_marginals(data)

    def get_marginals(self, attrs: tuple[str, ...], values: tuple[Any, ...]) -> float:
        return self.marginals[attrs][self.marginals.get_ordering_function(attrs)(values)]

    def save(self, working_dir: str) -> None:
        marginals_file_path = os.path.join(working_dir, consts.MARGINALS_FILE_NAME)
        ObjectLoader.save(self.marginals, marginals_file_path)

    def distance(self, other: "Marginals") -> float:
        return Series((self.marginals[attr_key] - other.marginals[attr_key]).fillna(1).abs().mean()  # Think about it
                      for attr_key in self.marginals.keys if attr_key in other.marginals).mean()

    @staticmethod
    def __calc_marginals(data: DataFrame) -> MultiIndexSeries[str, Series[tuple[Any, ...], float]]:
        return MultiIndexSeries(
            data=Series({key: data.groupby([*key]).size() / len(data)
                         for key in itertools.combinations(data.columns[1:], 2)}))

    @staticmethod
    def __load_marginals(working_dir: str) -> MultiIndexSeries[str, Series[tuple[Any, ...], float]]:
        marginals_file_path = os.path.join(working_dir, consts.MARGINALS_FILE_NAME)
        marginals_resource_file_path = os.path.join(consts.RESOURCES_DIR_PATH, consts.MARGINALS_FILE_NAME)
        return ObjectLoader.load(marginals_file_path) if os.path.exists(marginals_file_path) \
            else ObjectLoader.load(str(marginals_resource_file_path))
