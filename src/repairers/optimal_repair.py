import numpy as np
import pandas as pd
import scipy as sp
from pandas import DataFrame

from src.violations.functional_dependency import FunctionalDependency, get_common_lhs, select_consensus, select_marriage


def repair_data(data: DataFrame, fds: list[FunctionalDependency]) -> DataFrame:
    non_trivial_fds = [fd for fd in fds if not fd.is_trivial]
    if not non_trivial_fds:
        return data

    common_lhs = get_common_lhs(non_trivial_fds)
    if common_lhs:
        return __common_lhs_repair(data, fds, common_lhs)

    consensus = select_consensus(non_trivial_fds)
    if consensus:
        return __consensus_repair(data, fds, consensus)

    marriage = select_marriage(non_trivial_fds)
    if marriage:
        return __marriage_repair(data, fds, marriage)


def __common_lhs_repair(data: DataFrame, fds: list[FunctionalDependency], common_lhs: list[str]) -> DataFrame:
    no_commons_fds = [fd.remove_attrs(common_lhs) for fd in fds]
    return pd.concat([repair_data(block, no_commons_fds) for _, block in data.groupby(common_lhs)])


def __consensus_repair(data: DataFrame, fds: list[FunctionalDependency], consensus: list[str]) -> DataFrame:
    no_consensus_fds = [fd.remove_attrs(consensus) for fd in fds]
    return max(
        (repair_data(block, no_consensus_fds) for _, block in data.groupby(consensus)),
        key=len,
        default=None
    )


def __marriage_repair(data: DataFrame, fds: list[FunctionalDependency],
                      marriage: tuple[list[str], list[str]]) -> DataFrame:
    marriage_attrs = [*marriage[0], *marriage[1]]
    no_marriage_fds = [fd.remove_attrs(marriage_attrs) for fd in fds]
    repairs = {value: repair_data(block, no_marriage_fds) for value, block in data.groupby(marriage_attrs)}
    first_domain = data[marriage[0]].unique()
    second_domain = data[marriage[1]].unique()
    weights = np.array([[len(repairs[(a, b)]) for b in second_domain] for a in first_domain])
    costs = -weights
    first_domain_inds, second_domain_inds = sp.optimize.linear_sum_assignment(costs)
    return pd.concat([
        repairs[(first_domain[first_domain_ind], second_domain[second_domain_ind])]
        for first_domain_ind, second_domain_ind in zip(first_domain_inds, second_domain_inds)])
