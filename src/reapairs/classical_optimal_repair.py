import pandas as pd
from narwhals import DataFrame

from src.constraints.functional_dependencies import FunctionalDependencies


# No support in Marriage fds

def repair_data(data: DataFrame, fds: FunctionalDependencies) -> DataFrame:
    non_trivial_fds = fds.non_trivial_fds
    if not non_trivial_fds.is_empty:
        return data

    common_lhs = non_trivial_fds.common_lhs
    if common_lhs:
        return common_lhs_repair(data, non_trivial_fds, common_lhs)

    consensuses = non_trivial_fds.consensuses
    if consensuses:
        return consensus_repair(data, non_trivial_fds, consensuses)

    raise Exception("Problem is np hard")


def common_lhs_repair(data: DataFrame, fds: FunctionalDependencies, common_lhs: list[str]) -> DataFrame:
    no_commons_fds = fds.remove_attrs(common_lhs)
    return pd.concat([repair_data(block, no_commons_fds) for _, block in data.groupby(common_lhs)])


def consensus_repair(data: DataFrame, fds: FunctionalDependencies, consensus: list[str]) -> DataFrame:
    no_consensus_fds = fds.remove_attrs(consensus)
    return max(
        (repair_data(block, no_consensus_fds) for _, block in data.groupby(consensus)),
        key=len,
        default=None
    )
