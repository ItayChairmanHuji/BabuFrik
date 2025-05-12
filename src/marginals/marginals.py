import itertools
from typing import Any

import numpy as np
from pandas import DataFrame, Series

from src.marginals.multi_index_series import MultiIndexSeries


class Marginals:
    def __init__(self, data: DataFrame):
        self.marginals = self.__calc_marginals(data)

    def get_marginals(self, attrs: tuple[str, ...], values: tuple[Any, ...]) -> float:
        attrs_marginals = self.marginals[attrs]
        values_indices = self.marginals.get_ordering_function(attrs)(values)
        return attrs_marginals[values_indices] if values_indices in attrs_marginals else 0

    def mean_distance(self, other: "Marginals") -> float:
        attrs_distances = list(self.distance(other).values())
        return float(np.mean(attrs_distances))

    def distance(self, other: "Marginals") -> dict[tuple[str, ...], float]:
        return {attr_key: self.__attr_distance(other, attr_key)
                for attr_key in self.marginals.keys if attr_key in other.marginals}

    def __attr_distance(self, other: "Marginals", attr_key: tuple[str, ...]) -> float:
        return self.marginals[attr_key].sub(other.marginals[attr_key], fill_value=0).abs().mean()

    @staticmethod
    def __calc_marginals(data: DataFrame) -> MultiIndexSeries[str, Series]:
        return MultiIndexSeries({key: data.groupby([*key]).size() / len(data)
                                 for key in itertools.combinations(data.columns, 2)})
