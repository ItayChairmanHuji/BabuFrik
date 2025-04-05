import itertools
from typing import Any

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
        return Series(self.marginals[attr_key].sub(other.marginals[attr_key], fill_value=0).abs().mean()
                      for attr_key in self.marginals.keys if attr_key in other.marginals).mean()

    def distance(self, other: "Marginals") -> Series[float]:
        return Series(self.marginals[attr_key].sub(other.marginals[attr_key], fill_value=0).abs().mean()
                      for attr_key in self.marginals.keys if attr_key in other.marginals)

    @staticmethod
    def __calc_marginals(data: DataFrame) -> MultiIndexSeries[str, Series]:
        return MultiIndexSeries({key: data.groupby([*key]).size() / len(data)
                                 for key in itertools.combinations(data.columns, 2)})
