import json
from typing import Any, Callable

import gurobipy as gp
from pandas import DataFrame

from src import violations_finder
from src.constraints.functional_dependencies import FunctionalDependencies
from src.marginals.marginals import Marginals


def repair_data(data: DataFrame, fds: FunctionalDependencies,
                marginals: Marginals, license_file_path: str) -> DataFrame:
    model = create_model(license_file_path)
    objective = model.addVars(range(len(data)), vtype=gp.GRB.CONTINUOUS, name=[f"x_{i}" for i in range(len(data))])
    add_no_trivial_solution_constraint(model, objective)
    add_normalization_constraint(model, objective)
    no_violations_constraint_callback = lambda m, w: no_violations_constraint(m, w, objective, data, fds)
    weight_function = build_weight_function(data, marginals)
    weighted_sum = gp.quicksum(weight_function(i) * objective[i] for i in range(len(data)))
    model.setObjective(weighted_sum, gp.GRB.MINIMIZE)
    model.update()
    model.setParam(gp.GRB.Param.LazyConstraints, 1)
    model.optimize(no_violations_constraint_callback)
    return data.drop(index=[i for i in range(len(data)) if objective[i].X >= 0.5])


def create_model(license_file_path: str):
    license_params = json.load(open(license_file_path))
    env = gp.Env(params=license_params)
    model = gp.Model("VertexCover", env=env)
    model.setParam('OutputFlag', False)
    return model


def add_no_trivial_solution_constraint(model: gp.Model, objective: gp.tupledict[Any, gp.Var]) -> None:
    model.addConstr(objective.sum() >= 1)


def add_normalization_constraint(model: gp.Model, objective: gp.tupledict[Any, gp.Var]) -> None:
    for _, var in objective.items():
        model.addConstr(var >= 0)
        model.addConstr(var <= 1)


def no_violations_constraint(model: gp.Model, where: int, objective: gp.tupledict[Any, gp.Var],
                             data: DataFrame, fds: FunctionalDependencies) -> None:
    if where == gp.GRB.Callback.MIPSOL:
        x = model.cbGetSolution(objective)
        current_result = data.drop(index=[i for i in range(len(data)) if x[i] == 1])
        violating_tuples = violations_finder.find_violating_pairs(current_result, fds.fds)
        for i, j in violating_tuples:
            if x[i] + x[j] < 1: model.cbLazy(objective[i] + objective[j] >= 1)


def build_weight_function(data: DataFrame, marginals: Marginals) -> Callable[[int], float]:
    weights = {i: get_tuple_weight(data, i, marginals) for i in range(len(data))}
    return lambda i: weights[i]


def get_tuple_weight(data: DataFrame, tuple_index: int, marginals: Marginals) -> float:
    data_without_tuple = data.drop(index=tuple_index)
    marginals_without_tuple = Marginals(data_without_tuple)
    return marginals.mean_distance(marginals_without_tuple)
