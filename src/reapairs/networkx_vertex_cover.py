import itertools

import networkx as nx
import numpy as np
from networkx.algorithms.approximation.vertex_cover import min_weighted_vertex_cover
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
    return data.drop(min_weighted_vertex_cover(graph, w))
