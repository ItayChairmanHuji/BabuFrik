import os
from abc import abstractmethod, ABC
from dataclasses import dataclass

from matplotlib.figure import Figure

from src.storage import object_loader
from src.utils import consts
from src.utils.configuration import Configuration
from src.utils.report import Report


@dataclass
class Analyzer(ABC):
    working_dir: str
    figure: Figure
    config: Configuration
    fds_file_path: str
    marginals_errors_margins_file_path: str

    @property
    def name(self):
        return self.__module__.split(".")[-1]

    @staticmethod
    @abstractmethod
    def mandatory_fields() -> list[str]:
        raise NotImplementedError("Mandatory fields not implemented.")

    @abstractmethod
    def analyze(self, reports: dict[str, list[Report]]) -> None:
        raise NotImplementedError()

    def save_results(self, results: dict[str, dict[float, float]], file_name: str) -> None:
        dir_path = os.path.join(self.working_dir, consts.RESULTS_DIR_NAME)
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(dir_path, file_name)
        object_loader.save(results, file_path)
