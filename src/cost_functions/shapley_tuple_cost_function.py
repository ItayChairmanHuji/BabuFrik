import itertools
from typing import Callable

import shap
from pandas import DataFrame

from src.marginals.marginals import Marginals


class ShapleyTupleCostFunction:
    # TODO: Fix
    def calculate_cost(self, data: DataFrame, marginals: Marginals) -> Callable[[int], float]:
        value_function = lambda s: marginals.mean_distance(Marginals(data.drop(index=s)))
        for t in range(len(data)):
            temp_data = data.drop(index=t)
            for size in range(len(temp_data)):
                itertools.combinations()
        shap_calculator = shap.Explainer(value_function)
        results = shap_calculator(data)

