import itertools

from pandas import DataFrame

from src.violations.functional_dependency import FunctionalDependency

ViolatingPair = tuple[int, int]


def count_functional_dependency_violations(data: DataFrame, fd: FunctionalDependency) -> int:
    grouped = data.groupby([fd.source, fd.target]).size().reset_index(name="PairCount")
    filtering_condition = lambda x: x[f"{fd.target}_x"] < x[f"{fd.target}_y"]
    merged = grouped.merge(right=grouped, on=fd.source)[filtering_condition]
    return sum(merged["PairCount_x"] * merged["PairCount_y"])


def generate_violations_report(violations: dict[FunctionalDependency, int]) -> str:
    return ''.join(f'{fd}: {violations} \n' for (fd, violations) in violations.items())


def find_violating_pairs(data: DataFrame, fds: list[FunctionalDependency]) -> set[ViolatingPair]:
    return set().union(*[__find_violating_pairs_for_fd(data, fd) for fd in fds])


def __find_violating_pairs_for_fd(data: DataFrame, fd: FunctionalDependency) -> set[ViolatingPair]:
    return set().union(*[__extract_violating_pairs(group, fd)
                         for _, group in data.groupby([fd.source]) if __get_num_of_unique_values(group, fd.target) > 1])


def __extract_violating_pairs(subdata: DataFrame, fd: FunctionalDependency) -> set[ViolatingPair]:
    indices = list(subdata.index)
    values = subdata[fd.target].values
    return {(indices[i], indices[j])
            for i, j in itertools.combinations(range(len(indices)), 2) if values[i] != values[j]}


def __get_num_of_unique_values(data: DataFrame, column: str) -> int:
    return len(data[column].drop_duplicates())
