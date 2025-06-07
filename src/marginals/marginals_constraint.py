from dataclasses import dataclass
from typing import Any

import numpy as np
from pandas import DataFrame


@dataclass
class MarginalsConstraint:
    values: dict[str, Any]
    threshold: float

    def count_values(self, data: DataFrame) -> int:
        return data[np.logical_and(data[attr] == value for attr, value in self.values.items())].count()

    def is_satisfied(self, data: DataFrame) -> bool:
        return self.count_values(data)  >= self.threshold * len(data)
