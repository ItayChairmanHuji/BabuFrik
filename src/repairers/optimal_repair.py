from pandas import DataFrame

from src.violations.functional_dependency import FunctionalDependency


def repair_data(data: DataFrame, fds: list[FunctionalDependency]) -> DataFrame:
    if len(fds) == 0:
        return data
