import itertools
import os
from typing import Any, Iterator

import gurobipy as gp
import numpy as np
import pandas as pd
from pandas import Series

from src.utils import consts
from src.utils.constraint_adders.constraint_adder import ConstraintAdder
from src.utils.ilp_model import ILPModel


class MarginalsConstraintAdder(ConstraintAdder):
    @staticmethod
    def add_constraint(ilp: ILPModel) -> ILPModel:
        attributes = ilp.data.columns
        marginals_limits = MarginalsConstraintAdder.__load_marginals_error_factors(ilp)
        for attrs in itertools.combinations(attributes, 2):
            MarginalsConstraintAdder.__add_constraint_for_attrs_pair(ilp, attrs, marginals_limits)
        return ilp

    @staticmethod
    def __load_marginals_error_factors(ilp: ILPModel) -> Series:
        file_name = ilp.config["marginals_error_factors_file_name"]
        file_path = os.path.join(consts.MARGINALS_ERROR_FACTORS_DIR_PATH, file_name)
        return pd.read_csv(str(file_path), index_col=[0, 1], skipinitialspace=True).squeeze()

    @staticmethod
    def __add_constraint_for_attrs_pair(ilp: ILPModel, attrs: tuple[str, ...], marginals_error_factors: Series) -> None:
        res_size = gp.quicksum(ilp.objective)
        domains = tuple(ilp.data[attr].unique() for attr in attrs)
        domains_cross = itertools.product(*domains)
        marginals_error_var = ilp.model.addVars(domains_cross, vtype=gp.GRB.CONTINUOUS, name="marginals_errors")
        MarginalsConstraintAdder.__set_marginals_error_limit(ilp, domains_cross, marginals_error_var, attrs, res_size)
        marginals_error_factor = MarginalsConstraintAdder.__get_attr_marginals_error_factor(attrs,
                                                                                            marginals_error_factors)
        MarginalsConstraintAdder.__set_avg_marginals_error_limit(ilp,
                                                                 marginals_error_var, res_size, marginals_error_factor)

    @staticmethod
    def __set_marginals_error_limit(ilp: ILPModel, domains_cross: Iterator[tuple[Any, ...]],
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
    def __get_attr_marginals_error_factor(attrs: tuple[str, ...], marginals_error_factors: Series) -> float:
        for attrs_combination in itertools.permutations(attrs):
            if attrs_combination in marginals_error_factors:
                return float(marginals_error_factors[attrs_combination])
        return 0

    @staticmethod
    def __set_avg_marginals_error_limit(ilp: ILPModel, marginals_error_var: gp.tupledict[int, gp.Var],
                                        res_size: gp.LinExpr, marginals_error_factor) -> None:
        avg_marginals_error = marginals_error_var.sum() / len(marginals_error_var)
        ilp.model.addConstrs(avg_marginals_error <= res_size * marginals_error_factor)
