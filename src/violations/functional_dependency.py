import json
from dataclasses import dataclass
from itertools import chain

import numpy as np


@dataclass
class FunctionalDependency:
    lhs: list[str]
    rhs: list[str]

    def __str__(self) -> str:
        return f'{self.lhs} -> {self.rhs}'

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def is_trivial(self) -> bool:
        return set(self.rhs) <= set(self.lhs)

    @property
    def is_consensus(self) -> bool:
        return len(self.lhs) == 0

    def remove_attrs(self, attrs: list[str]) -> 'FunctionalDependency':
        return FunctionalDependency(
            [lhs for lhs in self.lhs if lhs not in attrs],
            [rhs for rhs in self.rhs if rhs not in attrs]
        )


def load_fds(fds_file_path: str) -> list[FunctionalDependency]:
    return [FunctionalDependency(**fd) for fd in json.load(open(fds_file_path))]


def is_trivial_set(fds: list[FunctionalDependency]) -> bool:
    return all(fd.is_trivial for fd in fds)


def get_common_lhs(fds: list[FunctionalDependency]) -> list[str]:
    return list(np.bitwise_and.reduce([set(fd.lhs) for fd in fds]))


def get_consensuses(fds: list[FunctionalDependency]) -> list[str]:
    return list(chain.from_iterable([fd.rhs for fd in fds if fd.is_consensus]))
