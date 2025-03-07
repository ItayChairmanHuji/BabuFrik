import pickle
from typing import Any


class ObjectLoader:
    @staticmethod
    def load(file_path: str) -> Any:
        with open(file_path, 'rb') as f:
            return pickle.load(f)

    @staticmethod
    def save(value: Any, file_path: str) -> None:
        with open(file_path, 'wb') as file:
            pickle.dump(value, file, protocol=pickle.HIGHEST_PROTOCOL)
