from typing import TypeVar, Generic, Callable

import numpy as np
from pandas import Series

K = TypeVar('K')
V = TypeVar('V')


class MultiIndexSeries(Generic[K, V]):
    def __init__(self, data: dict[tuple[K, ...], V], default_value: V = None):
        self.data = Series({self.format_key(key): value for key, value in data.items()})
        self.default_value = default_value

    def __getitem__(self, key: tuple[K, ...]) -> V:
        series_key = self.format_key(key)
        return self.data[series_key] if series_key in self.data else self.default_value

    def __setitem__(self, key: tuple[K, ...], value: V) -> None:
        self.data[self.format_key(key)] = value

    def __len__(self) -> int:
        return len(self.data)

    def __contains__(self, key: tuple[K, ...]) -> bool:
        return key in self.data

    @staticmethod
    def format_key(key: tuple[K, ...]) -> tuple[K, ...]:
        return tuple(sorted(key))

    @staticmethod
    def get_ordering_function(key: tuple[K, ...]) -> Callable[[tuple[V, ...]], tuple[V, ...]]:
        return lambda x: tuple(map(x.__getitem__, np.argsort(key)))

    @property
    def keys(self) -> list[tuple[K, ...]]:
        return self.data.keys().tolist()
