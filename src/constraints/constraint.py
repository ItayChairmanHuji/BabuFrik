import itertools
from dataclasses import dataclass
from typing import Any

from pandas import DataFrame, Series

from src.constraints.predicate import Predicate


@dataclass
class Constraint:
    num_of_vars: int
    predicates_vars: dict[Predicate, tuple[int, int]]

    @property
    def predicates(self) -> list[Predicate]:
        return list(self.predicates_vars.keys())

    @property
    def equality_type_predicates(self) -> list[Predicate]:
        return [p for p in self.predicates if p.is_equality_type_constraint]

    def violating_tuples(self, data: DataFrame) -> list[tuple[int, ...]]:
        candidates = []
        relevant_data = data.drop(columns=list(self.attrs))
        equality_type_attrs = self.equality_type_attrs
        for values, subdata in relevant_data.groupby(equality_type_attrs):
            if not self.is_equality_type_violation(values, equality_type_attrs): continue
            candidates.extend(self.find_inequality_type_violation(subdata))
        return candidates

    @property
    def attrs(self) -> set[str]:
        res = set()
        for predicate in self.predicates:
            res.add(predicate.attr1)
            if predicate.attr2 is not None:
                res.add(predicate.attr2)
        return res

    @property
    def equality_type_attrs(self) -> set[str, ...]:
        res = set()
        for predicate in self.equality_type_predicates:
            res.add(predicate.attr1)
            if predicate.attr2 is not None:
                res.add(predicate.attr2)
        return res

    def is_equality_type_violation(self, values: tuple[Any, ...], attrs: set[str, ...]) -> bool:
        t = Series({attrs[i]: values[i] for i in range(len(attrs))})
        for predicate, vars in self.equality_type_predicates:
            if not predicate.is_satisfied(t, t): return False
        return True

    def find_inequality_type_violation(self, data: DataFrame) -> list[tuple[int, ...]]:
        res = []
        data = data.reset_index(drop=True)
        for tuples in itertools.combinations(range(len(data)), self.num_of_vars):
            if self.is_inequality_type_violation(data, tuples):
                res.append(tuples)
        return res

    def is_inequality_type_violation(self, data: DataFrame, tuples: tuple[int, ...]) -> bool:
        for predicate, vars in self.predicates_vars.items():
            if predicate.is_equality_type_constraint:
                continue
            t, s = tuples[vars[0]], tuples[vars[1]]
            if not predicate.is_satisfied(data.iloc[t], data.iloc[s]): return False
        return True
