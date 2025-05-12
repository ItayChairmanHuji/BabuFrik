from dataclasses import dataclass


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

    @property
    def attributes(self) -> set[str]:
        return set(self.lhs) | set(self.rhs)

    def remove_attrs(self, attrs: list[str]) -> 'FunctionalDependency':
        self.lhs = [lhs for lhs in self.lhs if lhs not in attrs]
        self.rhs = [rhs for rhs in self.rhs if rhs not in attrs]
        return self
