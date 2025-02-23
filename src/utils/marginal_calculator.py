import itertools
from functools import reduce

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
