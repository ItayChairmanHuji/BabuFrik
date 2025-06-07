from dataclasses import dataclass

from pandas import DataFrame

from src.marginals.marginals_constraint import MarginalsConstraint


@dataclass
class MarginalsConstraints:
    constraints: list[MarginalsConstraint]

    def count_satisfied(self, data: DataFrame) -> int:
        return sum(constraint.is_satisfied(data) for constraint in self.constraints)

    def is_satisfied(self, data: DataFrame) -> bool:
        return all(constraint.is_satisfied(data) for constraint in self.constraints)
