import json
import pickle
from typing import Any


def load(file_path: str) -> Any:
    with open(file_path, 'rb') as f:
        return pickle.load(f)


def save(value: Any, file_path: str) -> None:
    with open(file_path, 'wb') as file:
        pickle.dump(value, file, protocol=pickle.HIGHEST_PROTOCOL)


def json_load(file_path: str) -> dict[str, Any]:
    with open(file_path, 'r') as f:
        return json.load(f)
