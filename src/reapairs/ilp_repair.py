import itertools
import json
from typing import Any

import gurobipy as gp
import numpy as np
from pandas import DataFrame

from src.marginals.marginals import Marginals
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src import violations_finder
from src.constraints.functional_dependencies import FunctionalDependencies


def repair_data(data: DataFrame, fds: FunctionalDependencies,
                marginals_error_margins: MarginalsErrorsMargins, marginals: Marginals,
                license_file_path: str) -> DataFrame:
    model = create_model(license_file_path)
    objective = model.addVars(range(len(data)), vtype=gp.GRB.BINARY, name=[f"x_{i}" for i in range(len(data))])
    add_no_trivial_solution_constraint(model, objective)
    no_violations_constraint_callback = lambda m, w: no_violations_constraint(m, w, objective, data, fds)
    add_marginals_conservation_constraint(data, model, objective, marginals, marginals_error_margins)
    model.setObjective(objective.sum(), gp.GRB.MAXIMIZE)
    model.update()
    model.optimize(no_violations_constraint_callback)
    return data.drop(index=[i for i in range(len(data)) if objective[i] == 0])


def create_model(license_file_path: str):
    license_params = json.load(open(license_file_path))
    env = gp.Env(params=license_params)
    model = gp.Model("ILP", env=env)
    model.setParam('OutputFlag', False)
    return model


def add_no_trivial_solution_constraint(model: gp.Model, objective: gp.tupledict[Any, gp.Var]) -> None:
    model.addConstr(objective.sum() >= 1)


def no_violations_constraint(model: gp.Model, where: int, objective: gp.tupledict[Any, gp.Var],
                             data: DataFrame, fds: FunctionalDependencies) -> None:
    if where == gp.GRB.Callback.MIPSOL:
        x = model.cbGetSolution(objective)
        current_result = data.drop(index=[i for i in range(len(data)) if x[i] == 0])
        violating_tuples = violations_finder.find_violating_pairs(current_result, fds.fds)
        for i, j in violating_tuples:
            if x[i] + x[j] > 1: model.cbLazy(objective[i] + objective[j] <= 1)


def add_marginals_conservation_constraint(data: DataFrame, model: gp.Model,
                                          objective: gp.tupledict[Any, gp.Var], marginals: Marginals,
                                          marginals_error_margins: MarginalsErrorsMargins) -> None:
    attributes = data.columns
    repaired_data_size = gp.quicksum(objective)
    for attrs in itertools.combinations(attributes, 2):
        domains = tuple(data[attr].unique() for attr in attrs)

        marginals_errors_vars = [calc_marginals_error_var(marginals, attrs, values, repaired_data_size) for values in
                                 itertools.product(*domains)]
        marginals_errors = gp.quicksum(marginals_errors_vars)
        model.addConstr(marginals_errors <= marginals_error_margins[attrs]
                        * len(marginals_errors_vars) * repaired_data_size, name="marginal_consistency")


def calc_marginals_error_var(data: DataFrame, model: gp.Model,
                             objective: gp.tupledict[Any, gp.Var], marginals: Marginals,
                             attrs: tuple[str, ...], values: tuple[Any, ...],
                             repaired_data_size: gp.LinExpr) -> gp.Var:
    matching_tuples_indices = data.index[
        np.logical_and.reduce([data[attr] == value for attr, value in zip(attrs, values)])]
    matching_tuples_size = gp.quicksum(objective[i] for i in matching_tuples_indices)
    marginals_error_var = model.addVar(vtype=gp.GRB.CONTINUOUS, name=f"abs_diff_{list(attrs)}_{list(values)}")
    marginals_value = marginals.get_marginals(attrs, values)
    model.addConstr(marginals_error_var >= marginals_value * repaired_data_size - matching_tuples_size)
    model.addConstr(marginals_error_var >= matching_tuples_size - marginals_value * repaired_data_size)
    return marginals_error_var
