import os
import uuid
from dataclasses import dataclass


@dataclass
class TempDir:
    base_path: str
    id: str = ""

    @property
    def path(self) -> str:
        path = os.path.join(self.base_path, self.id)
        os.makedirs(path, exist_ok=True)
        return path

    def reset(self) -> None:
        self.id = str(uuid.uuid4())
