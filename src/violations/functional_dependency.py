import json
from dataclasses import dataclass


@dataclass
class FunctionalDependency:
    source: str
    target: str

    def __str__(self) -> str:
        return f'{self.source} -> {self.target}'

    def __repr__(self) -> str:
        return self.__str__()


def load_fds(fds_file_path: str) -> list[FunctionalDependency]:
    return [FunctionalDependency(**fd) for fd in json.load(open(fds_file_path))]


def is_common_left_hand_side(fds: list[FunctionalDependency]) -> bool:
    return len({fd.source for fd in fds}) == 1
