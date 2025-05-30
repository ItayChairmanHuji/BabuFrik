import itertools
import time
from typing import Callable

import networkx as nx
from pandas import DataFrame

from src.constraints import violations_finder
from src.constraints.functional_dependencies import FunctionalDependencies
from src.entities.statistics import Statistics
from src.marginals.marginals import Marginals


def create_violations_graph(data: DataFrame, fds: FunctionalDependencies) -> nx.Graph:
    graph = nx.Graph()
    graph.add_nodes_from(data.index)
    violations = violations_finder.find_violating_tuples(data, fds)
    for violating_tuples in violations:
        for violating_pairs in itertools.combinations(violating_tuples, 2):
            graph.add_edge(*violating_pairs)
    return graph


def run_with_statistics(func: Callable[[], DataFrame],
                        fds: FunctionalDependencies, marginals: Marginals) -> tuple[DataFrame, Statistics]:
    start_time = time.time()
    result = func()
    runtime = time.time() - start_time
    violations = sum(violations_finder.count_functional_dependency_violations(result, fd) for fd in fds)
    marginals_difference = marginals.mean_distance(Marginals(result))
    return result, Statistics(runtime, violations, marginals_difference)
