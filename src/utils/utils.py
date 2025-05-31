import itertools
import time
from typing import Callable, Any

import networkx as nx
from pandas import DataFrame

from src.constraints import violations_finder
from src.constraints.functional_dependencies import FunctionalDependencies


def create_violations_graph(data: DataFrame, fds: FunctionalDependencies) -> nx.Graph:
    graph = nx.Graph()
    graph.add_nodes_from(data.index)
    violations = violations_finder.find_violating_tuples(data, fds)
    for violating_tuples in violations:
        for violating_pairs in itertools.combinations(violating_tuples, 2):
            graph.add_edge(*violating_pairs)
    return graph


def run_with_runtime(func: Callable[[], Any]) -> tuple[Any, float]:
    start_time = time.time()
    result = func()
    runtime = time.time() - start_time
    return result, runtime
