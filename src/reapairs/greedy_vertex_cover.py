import itertools

import networkx as nx
import numpy as np
from pandas import DataFrame

from src import violations_finder
from src.constraints.functional_dependencies import FunctionalDependencies
from src.marginals.marginals import Marginals
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins


def repair_data(data: DataFrame, fds: FunctionalDependencies,
                marginals: Marginals, marginals_errors_margins: MarginalsErrorsMargins) -> DataFrame:
    graph = nx.Graph()
    graph.add_nodes_from(data.index)
    violations = violations_finder.find_violating_tuples(data, fds)
    for violating_tuples in violations:
        for violating_pairs in itertools.combinations(violating_tuples, 2):
            graph.add_edge(*violating_pairs)

    w = np.ones(graph.number_of_nodes())  # Will be changed
    result = data
    while graph.number_of_nodes() > 0:
        factor = w / graph.degree
        current_vertex = np.argmin(factor)
        current_weight = factor[current_vertex]
        for neighbor in graph.neighbors(current_vertex):
            w[neighbor] = w[neighbor] - current_weight
            graph.remove_edge(neighbor, current_vertex)
        w[current_vertex] = 0
        result = data.drop(current_vertex, axis=0)
        graph.remove_nodes_from(list(nx.isolates(graph)))
    return result
