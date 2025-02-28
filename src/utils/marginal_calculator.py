import itertools
import os
import pickle
from typing import Any, Union

from pandas import DataFrame

from src.utils import consts


class MarginalsCalculator:
    Marginals = dict[tuple, DataFrame]

    @staticmethod
    def calculate_marginal(data: DataFrame) -> Marginals:
        attributes = data.columns[1:]
        return {
            attributes_pair: data.groupby([*attributes_pair]).size() / len(data)
            for attributes_pair in itertools.combinations(attributes, 2)
        }


class Marginals:
    AttributeKey = tuple[str, ...]
    ValuesKey = tuple[Any, ...]
    MarginalsType = dict[AttributeKey, DataFrame[ValuesKey, float]]

    def __init__(self, data: Union[DataFrame, str]):
        if not isinstance(data, DataFrame) and not isinstance(data, str):
            raise TypeError("data must be a DataFrame or a string")

        self.marginals = Marginals.__build_marginals_dict(data) \
            if isinstance(data, DataFrame) else Marginals.__load(data)

    def get_marginals(self, keys: dict[str, Any]) -> float:
        if len(keys) != 2:
            raise AttributeError("Can only calculate marginals for 2 attributes")

        attr_key, value_key = self.__get_marginals_keys(keys)
        return self.marginals[attr_key][value_key]

    def save(self, working_dir: str) -> None:
        marginals_file_path = os.path.join(working_dir, consts.MARGINALS_FILE_NAME)
        with open(marginals_file_path, "wb") as f:
            pickle.dump(self.marginals, f, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def __load(working_dir: str) -> MarginalsType:
        marginals_file_path = os.path.join(working_dir, consts.MARGINALS_FILE_NAME)
        marginals_resource_file_path = os.path.join(consts.RESOURCES_DIR_PATH, consts.MARGINALS_FILE_NAME)
        return pickle.load(open(marginals_file_path, "rb")) if os.path.exists(marginals_file_path) \
            else pickle.load(open(marginals_resource_file_path, "rb"))

    @staticmethod
    def __get_attribute_key(first_attribute: str, second_attribute: str) -> AttributeKey:
        return tuple(sorted([first_attribute, second_attribute]))

    @staticmethod
    def __get_marginals_keys(keys: dict[str, Any]) -> tuple[AttributeKey, ValuesKey]:
        attr_key = Marginals.__get_attribute_key(*keys.keys())
        value_key = tuple(keys[attr] for attr in attr_key)
        return attr_key, value_key

    @staticmethod
    def __build_marginals_dict(data: DataFrame) -> MarginalsType:
        return {(key := Marginals.__get_attribute_key(attr1, attr2)): Marginals.__calc_marginals(data, *key)
                for attr1, attr2 in itertools.combinations(data.columns[1:], 2)
                }

    @staticmethod
    def __calc_marginals(data: DataFrame, first_attribute: str, second_attribute: str) -> DataFrame:
        return data.groupby([first_attribute, second_attribute]).size() / len(data)
