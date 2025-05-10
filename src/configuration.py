from dataclasses import dataclass
from typing import Any, Union


@dataclass
class Configuration:
    dataset_name: str
    private_data_size: Union[int | list[int]]
    empty_values_threshold: float
    columns_threshold: int
    epsilon: float
    generator_name: str
    synthetic_data_size: Union[int | list[int]]
    unique_values_threshold: int
    generator_extra_data: dict[str, Any]
    generations_repeats: int
    repair_algorithm: str
    repair_repeats: int
    fds: Union[str, list[str]]
