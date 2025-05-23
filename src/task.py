from dataclasses import dataclass
from typing import Optional

from pandas import DataFrame

from src.action import Action
from src.constraints.functional_dependencies import FunctionalDependencies
from src.marginals.marginals import Marginals


@dataclass
class Task:
    data: DataFrame
    private_data_size: int
    synthetic_data_size: int
    fds: FunctionalDependencies
    action: Action
    marginals: Optional[Marginals] = None
