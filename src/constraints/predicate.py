from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Union

from pandas import Series, DataFrame

from src.constraints.operator import Operator, Sign


@dataclass
class Predicate(ABC):
    operator: Operator

    @abstractmethod
    def evaluate(self, data: Union[Series, DataFrame]) -> bool:
        raise NotImplementedError

    @property
    def is_equality(self) -> bool:
        return self.operator.sign == Sign.EQ

    @property
    @abstractmethod
    def attrs(self) -> list[str]:
        raise NotImplementedError


@dataclass
class SingleTuplePredicate(Predicate):
    tuple_id: int
    attr_name: str
    value: Any

    def evaluate(self, data: Union[Series, DataFrame]) -> bool:
        return self.operator.operate(data.iloc[self.tuple_id][self.attr_name], self.value)

    @property
    def attrs(self) -> list[str]:
        return [self.attr_name]


@dataclass
class MultiTuplePredicate(Predicate):
    first_tuple_id: int
    second_tuple_id: int
    first_attr_name: str
    second_attr_name: str

    def evaluate(self, data: Union[Series, DataFrame]) -> bool:
        return self.operator.operate(data.iloc[self.first_tuple_id][self.first_attr_name]
                                     , data.iloc[self.second_tuple_id][self.second_attr_name])

    @property
    def attrs(self) -> list[str]:
        return [self.first_attr_name, self.second_attr_name]