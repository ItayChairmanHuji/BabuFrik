from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable


class Sign(Enum):
    EQ = '=='
    NE = '!='
    GT = '>>'
    GE = '>='
    LT = '<<'
    LE = '<='


def choose_operation(sign: Sign) -> Callable[[Any, Any], bool]:
    match sign:
        case Sign.EQ:
            return lambda x, y: x == y
        case Sign.NE:
            return lambda x, y: x != y
        case Sign.GT:
            return lambda x, y: x > y
        case Sign.GE:
            return lambda x, y: x >= y
        case Sign.LT:
            return lambda x, y: x < y
        case Sign.LE:
            return lambda x, y: x <= y
        case _:
            raise ValueError(f'Invalid operator: {sign}')


@dataclass
class Operator:
    sign: Sign

    def operate(self, x: Any, y: Any) -> bool:
        return choose_operation(self.sign)(x, y)
