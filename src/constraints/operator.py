from dataclasses import dataclass
from typing import Any, Callable


def choose_operation(sign: str) -> Callable[[Any, Any], bool]:
    match sign:
        case '==':
            return lambda x, y: x == y
        case '!=':
            return lambda x, y: x != y
        case '>>':
            return lambda x, y: x > y
        case '>=':
            return lambda x, y: x >= y
        case '<<':
            return lambda x, y: x < y
        case '<=':
            return lambda x, y: x <= y
        case _:
            raise ValueError(f'Invalid operator: {sign}')


@dataclass
class Operator:
    sign: str

    def operate(self, x: Any, y: Any) -> bool:
        return choose_operation(self.sign)(x, y)
