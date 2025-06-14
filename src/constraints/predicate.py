from dataclasses import dataclass
from typing import Any

from pandas import Series

from src.constraints.operator import Operator


@dataclass
class Predicate:
    operator: Operator
    attr1: str
    attr2: str = None
    const: Any = None

    def is_satisfied(self, t: Series, s: Series) -> bool:
        if self.attr2 is None and self.const is None:
            raise Exception("Predicate must have at least one of attr2 or const")
        elif self.const is None:
            return self.operator.operate(t[self.attr1], s[self.attr2])
        elif self.attr2 is None:
            return self.operator.operate(t[self.attr1], self.const)
        else:
            raise Exception("Predicate can't have both attr2 or const")

    @property
    def is_equality_type_constraint(self) -> bool:
        return self.operator.sign == '=' or self.operator.sign == '!='
