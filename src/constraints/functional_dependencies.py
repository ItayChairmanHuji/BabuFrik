from dataclasses import dataclass
from itertools import chain
from typing import Iterator

import numpy as np

from src.constraints.functional_dependency import FunctionalDependency


@dataclass
class FunctionalDependencies:
    fds: list[FunctionalDependency]

    def __len__(self) -> int:
        return len(self.fds)

    def __iter__(self) -> Iterator[FunctionalDependency]:
        return iter(self.fds)

    @property
    def is_empty(self) -> bool:
        return len(self.fds) == 0

    @property
    def is_trivial(self) -> bool:
        return all(fd.is_trivial for fd in self.fds)

    @property
    def common_lhs(self) -> list[str]:
        return list(np.bitwise_and.reduce([set(fd.lhs) for fd in self.fds]))

    @property
    def consensuses(self) -> list[str]:
        return list(chain.from_iterable([fd.rhs for fd in self.fds if fd.is_consensus]))

    @property
    def attributes(self) -> set[str]:
        return set(chain.from_iterable(fd.attributes for fd in self.fds))

    @property
    def non_trivial_fds(self) -> 'FunctionalDependencies':
        return FunctionalDependencies([fd for fd in self.fds if not fd.is_trivial])

    def remove_attrs(self, attrs: list[str]) -> 'FunctionalDependencies':
        return FunctionalDependencies([fd.remove_attrs(attrs) for fd in self.fds])
