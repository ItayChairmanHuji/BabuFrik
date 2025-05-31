from pandas import DataFrame

from src.actors import repairing
from src.constraints.functional_dependencies import FunctionalDependencies
from src.marginals.marginals import Marginals
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins


def calculate_quality(synthetic_dataset: DataFrame, repaired_dataset: DataFrame, marginals: Marginals,
                      fds: FunctionalDependencies, marginals_error_margins: MarginalsErrorsMargins) -> float:
    optimal_repair = repairing.repair_data(synthetic_dataset, fds, marginals, marginals_error_margins, "ilp")
    num_of_removed_tuples_in_optimal_solution = len(synthetic_dataset) - len(optimal_repair)
    num_of_removed_tuples_in_heuristic_solution = len(synthetic_dataset) - len(repaired_dataset)
    return num_of_removed_tuples_in_heuristic_solution / num_of_removed_tuples_in_optimal_solution - 1
