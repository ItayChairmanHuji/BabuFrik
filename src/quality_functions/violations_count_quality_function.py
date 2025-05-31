from pandas import DataFrame

from src.constraints import violations_finder
from src.constraints.functional_dependencies import FunctionalDependencies


def calculate_quality(synthetic_dataset: DataFrame, fds: FunctionalDependencies) -> float:
    return sum(violations_finder.count_functional_dependency_violations(synthetic_dataset, fd) for fd in fds)
