import itertools
from functools import reduce
from typing import Any

from pandas import DataFrame


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

    def __init__(self, data: DataFrame):
        self.marginals = {
            (key := self.__get_attribute_key(attr1, attr2)): self.__calc_marginals(data, *key)
            for attr1, attr2 in itertools.combinations(data.columns[1:], 2)
        }

    def get_marginals(self, keys: dict[str, Any]) -> float:
        if len(keys) != 2:
            raise AttributeError("Can only calculate marginals for 2 attributes")

        attr_key, value_key = self.__get_marginals_keys(keys)
        return self.marginals[attr_key][value_key]

    @staticmethod
    def __get_attribute_key(first_attribute: str, second_attribute: str) -> AttributeKey:
        return tuple(sorted([first_attribute, second_attribute]))

    @staticmethod
    def __get_marginals_keys(keys: dict[str, Any]) -> tuple[AttributeKey, ValuesKey]:
        attr_key = Marginals.__get_attribute_key(*keys.keys())
        value_key = tuple(keys[attr] for attr in attr_key)
        return attr_key, value_key

    @staticmethod
    def __calc_marginals(data: DataFrame, first_attribute: str, second_attribute: str) -> DataFrame:
        return data.groupby([first_attribute, second_attribute]).size() / len(data)