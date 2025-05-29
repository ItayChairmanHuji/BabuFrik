from pandas import DataFrame

from src.constraints.functional_dependencies import FunctionalDependencies
from src.marginals.marginals import Marginals
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.quality_functions.quality_function import QualityFunction


class MarginalsDistanceQualityFunction(QualityFunction):
    def calculate_quality(self, private_dataset: DataFrame, synthetic_dataset: DataFrame,
                          repaired_dataset: DataFrame,marginals: Marginals, fds: FunctionalDependencies,
                          marginals_error_margins: MarginalsErrorsMargins, target_attribute: str) -> float:
        return marginals.mean_distance(Marginals(repaired_dataset))
