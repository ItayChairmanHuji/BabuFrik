from dataclasses import dataclass
from typing import Any


@dataclass
class Message:
    from_service: str = None
    to_service: str = None
    data_file_path: str = None
    extra_data: dict[str, Any] = None
