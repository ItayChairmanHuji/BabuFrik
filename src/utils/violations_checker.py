import itertools

from pandas import DataFrame

from functional_dependency import FunctionalDependency


class ViolationsChecker:
    ViolatingPair = tuple[int, int]

    @staticmethod
    def count_functional_dependency_violations(data: DataFrame, fd: FunctionalDependency) -> int:
        grouped = data.groupby([fd.source, fd.target]).size().reset_index(name="PairCount")
        filtering_condition = lambda x: x[f"{fd.target}_x"] < x[f"{fd.target}_y"]
        merged = grouped.merge(right=grouped, on=fd.source)[filtering_condition]
        return sum(merged["PairCount_x"] * merged["PairCount_y"])

    @staticmethod
    def generate_violations_report(violations: dict[FunctionalDependency, int]) -> str:
        return ''.join(f'{fd}: {violations} \n' for (fd, violations) in violations.items())

    @staticmethod
    def find_violating_pairs(data: DataFrame, fds: list[FunctionalDependency]) -> set[ViolatingPair]:
        return set().union(
            *[ViolationsChecker.__find_violating_pairs_for_fd(data, fd)
              for fd in fds])

    @staticmethod
    def __find_violating_pairs_for_fd(data: DataFrame, fd: FunctionalDependency) -> set[ViolatingPair]:
        return set().union(
            *[ViolationsChecker.__extract_violating_pairs(group, fd)
              for _, group in data.groupby([fd.source])
              if ViolationsChecker.__get_num_of_unique_values(group, fd.target) > 1]
        )

    @staticmethod
    def __extract_violating_pairs(subdata: DataFrame, fd: FunctionalDependency) -> set[ViolatingPair]:
        indices = list(subdata.index)
        values = subdata[fd.target].values
        return {(indices[i], indices[j]) for i, j in itertools.combinations(range(len(indices)), 2) if
                values[i] != values[j]}

    @staticmethod
    def __get_num_of_unique_values(data: DataFrame, column: str) -> int:
        return len(data[column].drop_duplicates())
