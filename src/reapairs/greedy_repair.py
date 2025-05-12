import numpy as np
from pandas import DataFrame
from src.constraints.functional_dependencies import FunctionalDependencies

from src.marginals.marginals import Marginals
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.reapairs import classical_optimal_repair


def repair_data(data: DataFrame, fds: FunctionalDependencies,
                marginals: Marginals, marginals_errors_margins: MarginalsErrorsMargins) -> DataFrame:
    repaired_data = classical_optimal_repair.repair_data(data, fds)
    while should_remove_tuple(repaired_data, marginals, marginals_errors_margins):
        tuple_to_remove = find_tuple_to_remove(repaired_data, marginals)
        repaired_data.drop(index=tuple_to_remove)
    return repaired_data


def should_remove_tuple(data: DataFrame, marginals: Marginals,
                        marginals_errors_margins: MarginalsErrorsMargins) -> bool:
    return any(distance > marginals_errors_margins[attrs]
               for attrs, distance in marginals.distance(Marginals(data)).items())


def find_tuple_to_remove(data: DataFrame, marginals: Marginals) -> int:
    return int(np.argmin(calculate_tuple_cost(data, ind, marginals) for ind in range(len(data))))


def calculate_tuple_cost(data: DataFrame, tuple_index: int, marginals: Marginals) -> float:
    data_without_tuple = data.drop(index=tuple_index)
    marginals_without_tuple = Marginals(data_without_tuple)
    return marginals.mean_distance(marginals_without_tuple)
