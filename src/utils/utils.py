import json
import time
from typing import Callable

from pandas import DataFrame

from src.constraints import violations_finder
from src.constraints.functional_dependencies import FunctionalDependencies
from src.constraints.functional_dependency import FunctionalDependency
from src.marginals.marginals import Marginals
from src.entities.statistics import Statistics


def load_fds_file(fds_file_path: str) -> FunctionalDependencies:
    return FunctionalDependencies([FunctionalDependency(**fd) for fd in json.load(open(fds_file_path))])


def input_type_validation(list_type_value, *str_type_values) -> bool:
    return isinstance(list_type_value, list) and all(
        isinstance(str_type_value, (str, int)) for str_type_value in str_type_values)


def run_with_statistics(func: Callable[[], DataFrame],
                        fds: FunctionalDependencies, marginals: Marginals) -> tuple[DataFrame, Statistics]:
    start_time = time.time()
    result = func()
    runtime = time.time() - start_time
    violations = sum(violations_finder.count_functional_dependency_violations(result, fd) for fd in fds)
    marginals_difference = marginals.mean_distance(Marginals(result))
    return result, Statistics(runtime, violations, marginals_difference)
