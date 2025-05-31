from pandas import DataFrame

from src.marginals.marginals import Marginals


def calculate_quality(repaired_dataset: DataFrame, marginals: Marginals) -> float:
    return marginals.mean_distance(Marginals(repaired_dataset))
