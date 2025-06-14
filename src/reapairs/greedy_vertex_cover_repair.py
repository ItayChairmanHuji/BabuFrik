import itertools
import random
import time

import networkx as nx
import pandas as pd
from matplotlib import pyplot as plt
from pandas import DataFrame

from src.constraints import violations_finder, functional_dependencies
from src.constraints.functional_dependencies import FunctionalDependencies
from src.constraints.functional_dependency import FunctionalDependency


def repair_data(data: DataFrame, fds: FunctionalDependencies) -> DataFrame:
    graph = create_violations_graph(data, fds)
    isolated = list(nx.isolates(graph))
    graph.remove_nodes_from(isolated)
    w = {node: random.randint(1, len(data)) for node in graph.nodes}  # cost_function.calculate_cost(data, marginals)
    for node, value in w.items():
        if value == 0:
            print(node)
            print(graph.degree[node])
    while graph.number_of_edges() > 0:
        vertex, factor = min(w.items(), key=lambda x: x[1] / graph.degree[x[0]])
        data = data.drop(index=vertex)
        for neighbor in graph.neighbors(vertex):
            w[neighbor] -= factor
            if graph.number_of_edges(neighbor) == 1:
                graph.remove_node(neighbor)
                del w[neighbor]
        graph.remove_node(vertex)
        isolated = list(nx.isolates(graph))
        graph.remove_nodes_from(isolated)
        del w[vertex]
        for v in isolated:
            del w[v]
    return data


def create_violations_graph(data: DataFrame, fds: FunctionalDependencies) -> nx.Graph:
    graph = nx.Graph()
    graph.add_nodes_from(data.index)
    violations = violations_finder.find_violating_tuples(data, fds)
    for violating_tuples in violations:
        for violating_pairs in itertools.combinations(violating_tuples, 2):
            graph.add_edge(*violating_pairs)
    return graph


def find_violating_tuples(data: DataFrame, fds: FunctionalDependencies) -> list[list[int]]:
    res = []
    for fd in fds:
        fd_violations = find_violating_tuples_for_fd(data, fd)
        for violations in fd_violations:
            res.append(violations)
    return res


def find_violating_tuples_for_fd(data: DataFrame, fd: FunctionalDependency) -> list[list[int]]:
    res = []
    num_unique_rhs_per_group = data.groupby(fd.lhs)[fd.rhs].nunique()
    violating_keys = num_unique_rhs_per_group[num_unique_rhs_per_group > 1].dropna().index
    lhs_values = data.set_index(fd.lhs).index
    masks = [lhs_values == violating_key for violating_key in violating_keys]
    for mask in masks:
        res.append(data[mask].index.tolist())
    return res


def main():
    datapints = [10_000, 20_000, 50_000, 100_000]
    runtimes = []
    for datapoint in datapints:
        data = pd.read_csv(f"test_data/{datapoint}.csv")
        fds = functional_dependencies.load_fds_file("datasets/flight/fds.json")
        start_time = time.time()
        repair_data(data, fds)
        runtimes.append(time.time() - start_time)
    plt.plot(datapints, runtimes)