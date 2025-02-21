import itertools
from functools import reduce

from pandas import DataFrame


class MarginalsCalculator:
    Marginals = dict[tuple, float]
    MarginalsSet = dict[tuple, Marginals]

    @staticmethod
    def calculate_marginal(data: DataFrame) -> MarginalsSet:
        attributes = data.columns[1:]
        calc_marginals_for_attribute_pair = MarginalsCalculator.__calculate_marginals_for_attributes_pair
        return {
            attributes_pair: calc_marginals_for_attribute_pair(data, attributes_pair)
            for attributes_pair in itertools.combinations(attributes, 2)
        }

    @staticmethod
    def __calculate_marginals_for_attributes_pair(data: DataFrame, attributes_pair: tuple) -> Marginals:
        domains = (data[attribute].unique() for attribute in attributes_pair)
        calc_marginals_for_value_pair = MarginalsCalculator.__calculate_marginal_for_values_pair
        return {
            value_pair: calc_marginals_for_value_pair(data, attributes_pair, value_pair)
            for value_pair in itertools.product(*domains)
        }

    @staticmethod
    def __calculate_marginal_for_values_pair(data: DataFrame, attributes_pair: tuple, values_pair: tuple) -> float:
        return len(
            data.loc[
                reduce(lambda acc, pair: acc & (data[pair[0]] == pair[1]),
                       zip(attributes_pair, values_pair), True)
            ]
        )
