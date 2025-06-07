import networkx as nx
from pandas import DataFrame

from src.constraints.functional_dependencies import FunctionalDependencies
from src.cost_functions.cost_function import CostFunction
from src.marginals.marginals_constraints import MarginalsConstraints
from src.utils import utils


def repair_data(data: DataFrame, fds: FunctionalDependencies,
                marginals: MarginalsConstraints, cost_function: CostFunction) -> DataFrame:
    graph = utils.create_violations_graph(data, fds)
    while graph.number_of_edges() > 0:
        w = cost_function.calculate_cost(data, marginals)
        vertex, _ = min(w.items(), key=lambda x: x[1]/graph.degree[x])
        data = data.drop(index=vertex)
        graph.remove_node(vertex)
        graph.remove_nodes_from(list(nx.isolates(graph)))
    return data
