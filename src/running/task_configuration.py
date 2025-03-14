from dataclasses import dataclass
from typing import Any


@dataclass
class TaskConfiguration:
    services: list[str]
    analyzers: list[str]
    dynamic_fields: dict[str, list[Any]]
    functional_dependencies_file_name: str
    marginals_errors_margins_file_name: str
    publisher_name: str
