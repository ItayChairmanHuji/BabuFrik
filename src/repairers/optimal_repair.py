import pandas as pd
from pandas import DataFrame

from src.violations.functional_dependency import FunctionalDependency, get_common_lhs, get_consensuses


# No support in Marriage fds

def repair_data(data: DataFrame, fds: list[FunctionalDependency]) -> DataFrame:
    non_trivial_fds = [fd for fd in fds if not fd.is_trivial]
    if not non_trivial_fds:
        return data

    common_lhs = get_common_lhs(non_trivial_fds)
    if common_lhs:
        return __common_lhs_repair(data, non_trivial_fds, common_lhs)

    consensuses = get_consensuses(non_trivial_fds)
    if consensuses:
        return __consensus_repair(data, non_trivial_fds, consensuses)

    raise Exception("Problem is np hard")


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
