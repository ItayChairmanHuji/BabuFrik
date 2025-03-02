import json
import os
from dataclasses import dataclass

from src.utils import consts


@dataclass
class FunctionalDependency:
    source: str
    target: str

    def __str__(self) -> str:
        return f'{self.source} -> {self.target}'

    def __repr__(self) -> str:
        return self.__str__()


def load_fds(working_dir: str) -> list[FunctionalDependency]:
    fd_file = os.path.join(working_dir, consts.FUNCTIONAL_DEPENDENCIES_FILE_NAME)
    fds_as_json = json.load(open(fd_file))
    return [FunctionalDependency(**fd) for fd in fds_as_json]