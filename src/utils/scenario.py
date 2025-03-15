from dataclasses import dataclass
from typing import Any

from src.utils.message import Message


@dataclass
class Scenario:
    message: Message
    dynamic_fields: dict[str, Any]
