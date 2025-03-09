import itertools
from typing import Any

import gurobipy as gp
import numpy as np
from pandas import DataFrame

from src.ilp.optimal_data_repair_ilp import OptimalDataRepairILP
from src.marginals.marginals import Marginals
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.utils.configuration import Configuration
from src.violations.functional_dependency import FunctionalDependency


class SyntheticDataRepairILP(OptimalDataRepairILP):
    def __init__(self, data: DataFrame, fds: list[FunctionalDependency], config: Configuration, marginals: Marginals):
        super().__init__(data, fds, config)
        marginals_errors_margins = MarginalsErrorsMargins(config["marginals_error_margins_file_path"])
        self.__add_marginals_conservation_constraint(marginals, marginals_errors_margins)

    def __add_marginals_conservation_constraint(self, marginals: Marginals,
                                                marginals_error_margins: MarginalsErrorsMargins) -> None:
        attributes = self.data.columns
        repaired_data_size = gp.quicksum(self.objective)
        for attrs in itertools.combinations(attributes, 2):
            domains = tuple(self.data[attr].unique() for attr in attrs)

            marginals_errors_vars = [self.__calc_marginals_error_var(marginals, attrs, values, repaired_data_size)
                                     for values in itertools.product(*domains)]
            marginals_errors = gp.quicksum(marginals_errors_vars)
            self.model.addConstr(marginals_errors <= marginals_error_margins[attrs]
                                 * len(marginals_errors_vars) * repaired_data_size, name="marginal_consistency")

    def __calc_marginals_error_var(self, marginals: Marginals, attrs: tuple[str, ...],
                                   values: tuple[Any, ...], repaired_data_size: gp.LinExpr) -> gp.Var:
        matching_tuples_indices = self.data.index[np.logical_and.reduce([self.data[attr] == value
                                                                         for attr, value in zip(attrs, values)])]
        matching_tuples_size = gp.quicksum(self.objective[i] for i in matching_tuples_indices)
        marginals_error_var = self.model.addVar(vtype=gp.GRB.CONTINUOUS, name=f"abs_diff_{list(attrs)}_{list(values)}")
        marginals_value = marginals.get_marginals(attrs, values)
        self.model.addConstr(marginals_error_var >= marginals_value * repaired_data_size - matching_tuples_size)
        self.model.addConstr(marginals_error_var >= matching_tuples_size - marginals_value * repaired_data_size)
        return marginals_error_var
