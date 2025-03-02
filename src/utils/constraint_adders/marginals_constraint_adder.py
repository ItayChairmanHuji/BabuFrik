import itertools
from typing import Any

import gurobipy as gp
import numpy as np

from src.utils.constraint_adders.constraint_adder import ConstraintAdder
from src.utils.ilp_model import ILPModel


class MarginalsConstraintAdder(ConstraintAdder):
    @staticmethod
    def add_constraint(ilp: ILPModel) -> ILPModel:
        attributes = ilp.data.columns
        for attrs in itertools.combinations(attributes, 2):
            MarginalsConstraintAdder.__add_constraint_for_attrs_pair(ilp, attrs)
        return ilp

    @staticmethod
    def __add_constraint_for_attrs_pair(ilp: ILPModel, attrs: tuple[str, ...]) -> None:
        res_size = gp.quicksum(ilp.objective)
        domains = tuple(ilp.data[attr].unique() for attr in attrs)
        domains_cross = itertools.product(*domains)
        marginals_error_var = ilp.model.addVars(domains_cross, vtype=gp.GRB.CONTINUOUS, name="marginals_errors")
        MarginalsConstraintAdder.__set_marginals_error_limit(ilp, domains_cross, marginals_error_var, attrs, res_size)

    @staticmethod
    def __set_marginals_error_limit(ilp: ILPModel, domains_cross: itertools.product[tuple[Any, ...]],
                                    marginals_error_var: gp.tupledict[tuple[Any, ...], gp.Var], attrs: tuple[str, ...],
                                    res_size: gp.LinExpr) -> None:
        for values in domains_cross:
            marginals_errors_limit = MarginalsConstraintAdder.__get_marginals_error_limit(ilp, attrs, values, res_size)
            ilp.model.addConstr(marginals_error_var[values] <= marginals_errors_limit)
            ilp.model.addConstr(marginals_error_var[values] >= -marginals_errors_limit)

    @staticmethod
    def __get_marginals_error_limit(ilp: ILPModel, attrs: tuple[str, ...],
                                    values: tuple[Any, ...], res_size: gp.LinExpr) -> gp.LinExpr:
        marginals_key = {attr: value for attr, value in zip(attrs, values)}
        private_marginal = ilp.marginals.get_marginals(marginals_key)
        expected_size = private_marginal * res_size
        indices = ilp.data.index[np.logical_and(ilp.data[attr] == value for attr, value in zip(attrs, values))]
        actual_size = gp.quicksum(ilp.objective[indices])
        return actual_size - expected_size

    @staticmethod
    def __set_avg_marginals_error_limit(ilp: ILPModel, marginals_error_var: gp.tupledict[int, gp.Var],
                                        res_size: gp.LinExpr) -> None:
        marginals_error_factor = ilp.config["marginals_error_factor"]
        avg_marginals_error = marginals_error_var.sum() / len(marginals_error_var)
        ilp.model.addConstrs(avg_marginals_error <= res_size * marginals_error_factor)
