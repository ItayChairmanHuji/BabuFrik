from dataclasses import dataclass

from pandas import DataFrame

from src.constraints.functional_dependencies import FunctionalDependencies
from src.marginals.marginals import Marginals


@dataclass
class Task:
    private_data_size: int
    synthetic_data_size: int
    fds: FunctionalDependencies
    relative_num_of_private_marginals: float
    marginals_privacy_budget: float

    private_data: DataFrame = None
    synthetic_data: DataFrame = None
    repaired_data: DataFrame = None
    marginals: Marginals = None
