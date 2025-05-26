import numpy as np
from networkx.algorithms.approximation.vertex_cover import min_weighted_vertex_cover
from pandas import DataFrame

from src.constraints.functional_dependencies import FunctionalDependencies
from src.marginals.marginals import Marginals
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.utils import utils


def repair_data(data: DataFrame, fds: FunctionalDependencies,
                marginals: Marginals, marginals_errors_margins: MarginalsErrorsMargins) -> DataFrame:
    graph = utils.create_violations_graph(data, fds)
    w = np.ones(graph.number_of_nodes())  # Will be changed
    return data.drop(min_weighted_vertex_cover(graph, w))
