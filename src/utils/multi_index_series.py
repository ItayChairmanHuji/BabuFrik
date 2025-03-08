from dataclasses import dataclass
from typing import TypeVar, Generic, Callable

import numpy as np
from pandas import Series

K = TypeVar('K')
V = TypeVar('V')


@dataclass
class MultiIndexSeries(Generic[K, V]):
    data: Series[tuple[K, ...], V] = Series()
    default_value: V = None

    def __getitem__(self, key: tuple[K, ...]) -> V:
        series_key = sorted(key)
        return self.data[series_key] if series_key in self.data else self.default_value

    def __setitem__(self, key: tuple[K, ...], value: V) -> None:
        self.data[sorted(key)] = value

    def __len__(self) -> int:
        return len(self.data)

    @staticmethod
    def get_ordering_function(key: tuple[K, ...]) -> Callable[[tuple[V, ...]], tuple[V, ...]]:
        return lambda x: tuple(map(x.__getitem__, np.argsort(key)))

    @property
    def keys(self) -> list[tuple[K, ...]]:
        return self.data.keys().tolist()
