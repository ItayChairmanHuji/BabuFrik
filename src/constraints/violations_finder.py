from pandas import DataFrame

from src.constraints.functional_dependencies import FunctionalDependencies
from src.constraints.functional_dependency import FunctionalDependency


def count_functional_dependency_violations(data: DataFrame, fd: FunctionalDependency) -> int:
    grouped = data.groupby([*fd.lhs, *fd.rhs]).size().reset_index(name="PairCount")
    filtering_condition = lambda x: (
            x[[f"{rhs}_x" for rhs in fd.rhs]].squeeze(axis=1) < x[[f"{rhs}_y" for rhs in fd.rhs]].squeeze(axis=1))
    merged = grouped.merge(right=grouped, on=fd.lhs)[filtering_condition]
    return sum(merged["PairCount_x"] * merged["PairCount_y"])


def generate_violations_report(violations: dict[FunctionalDependency, int]) -> str:
    return ''.join(f'{fd}: {violations} \n' for (fd, violations) in violations.items())


def find_violating_tuples(data: DataFrame, fds: FunctionalDependencies) -> list[list[int]]:
    for fd in fds:
        fd_violations = find_violating_tuples_for_fd(data, fd)
        for violations in fd_violations:
            yield violations


def find_violating_tuples_for_fd(data: DataFrame, fd: FunctionalDependency) -> list[list[int]]:
    num_unique_rhs_per_group = data.groupby(fd.lhs)[fd.rhs].nunique()
    violating_keys = num_unique_rhs_per_group[num_unique_rhs_per_group > 1].dropna().index
    lhs_values = data.set_index(fd.lhs).index
    masks = [lhs_values == violating_key for violating_key in violating_keys]
    for mask in masks:
        yield data[mask].index.tolist()
