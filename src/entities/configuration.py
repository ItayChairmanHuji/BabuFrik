from dataclasses import dataclass
from typing import Any, Union, Optional

from src.entities.action import Action
from src.entities.algorithms import MarginalsGenerationAlgorithms, RepairAlgorithms, SynthesizingAlgorithms, \
    CostFunctions, QualityFunctions, NodesTypes
from src.entities.run_type import RunType


def input_type_validation(list_type_value, *str_type_values) -> bool:
    return isinstance(list_type_value, list) and all(
        isinstance(str_type_value, (str, int)) for str_type_value in str_type_values)


@dataclass
class Configuration:
    dataset_name: str
    synthesizing_algorithm: SynthesizingAlgorithms
    synthesizing_quality: QualityFunctions = QualityFunctions.NO_QUALITY
    marginals_algorithm: MarginalsGenerationAlgorithms = MarginalsGenerationAlgorithms.PUBLIC_MARGINALS
    marginals_quality: QualityFunctions = QualityFunctions.NO_QUALITY
    repairing_algorithm: RepairAlgorithms = RepairAlgorithms.NO_REPAIR
    repairing_quality: QualityFunctions = QualityFunctions.NO_QUALITY
    private_data_size: Union[int | list[int]] = 5_000
    synthetic_data_size: Union[int | list[int]] = 15_000
    fds: Union[str, list[str]] = "fds.json"
    marginals_privacy_budget: Union[float | list[float]] = 1
    relative_num_of_marginals: Union[float | list[float]] = 0.5
    cost_function: CostFunctions = CostFunctions.LEAVE_ONE_OUT
    repetitions: int = 10
    empty_values_threshold: float = 0.5
    columns_threshold: int = 100
    synthesizing_privacy_budget: float = 1
    synthesizing_extra_data: Optional[dict[str, Any]] = None
    num_of_tasks_in_parallel: int = 10
    monitored_object: NodesTypes = NodesTypes.REPAIRING

    @property
    def synthesizer_extra_data(self) -> dict:
        if self.synthesizing_extra_data is not None:
            return self.synthesizing_extra_data
        return {"regularization": "dragan"} if self.synthesizing_algorithm == SynthesizingAlgorithms.PATECTGAN else {}

    @property
    def run_type(self):
        possible_types = {
            RunType.PRIVATE_DATA: self.private_data_size,
            RunType.SYNTHETIC_DATA: self.synthetic_data_size,
            RunType.CONSTRAINTS_NUM: self.fds,
            RunType.MARGINALS_NUM: self.relative_num_of_marginals,
            RunType.PRIVACY_BUDGET: self.marginals_privacy_budget
        }
        for key, value in possible_types.items():
            temp = list(possible_types.copy().values())
            temp.remove(value)
            if input_type_validation(value, *temp):
                return key
        raise Exception("Invalid configuration input")

    @property
    def target_attribute(self) -> str:
        if self.dataset_name == "adult":
            return "income"
        elif self.dataset_name == "flight":
            return "actual_arrival"
        elif self.dataset_name == "compas":
            return "RecSupervisionLevel"
        raise Exception("Invalid configuration input")

    def quality_function(self, action: Action) -> QualityFunctions:
        match action:
            case Action.CLEANING:
                return QualityFunctions.NO_QUALITY
            case Action.SYNTHESIZING:
                return self.synthesizing_quality
            case Action.MARGINALS:
                return self.marginals_quality
            case Action.REPAIRING:
                return self.repairing_quality
