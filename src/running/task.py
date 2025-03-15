import os
from dataclasses import dataclass
from typing import Any

from matplotlib import pyplot as plt

from src.analyzers import analyzers_utils
from src.analyzers.analyzer import Analyzer
from src.running.job import Job
from src.utils import consts


@dataclass
class Task:
    working_dir: str
    job: Job
    dynamic_fields: dict[str, list[Any]]
    analyzers: list[Analyzer]

    def run(self) -> None:
        for job_index, dynamic_values in enumerate(zip(*self.dynamic_fields.values())):
            print(f"Running job {job_index}")
            dynamic_fields = {field: value for field, value in zip(self.dynamic_fields.keys(), dynamic_values)}
            self.job.run(self.working_dir, dynamic_fields)

    def __create_analyzer(self, analyzer_name: str) -> Analyzer:
        analyzer_class, analyzer_config = analyzers_utils.load_analyzer(analyzer_name, self.dynamic_fields)
        results_dir = os.path.join(self.working_dir, consts.RESULTS_DIR_NAME)
        os.makedirs(results_dir, exist_ok=True)
        return analyzer_class(
            working_dir=results_dir,
            figure=plt.figure(),
            fds_file_path=self.job.fds_file_path,
            marginals_errors_margins_file_path=self.job.marginals_errors_margins_file_path,
            config=analyzer_config
        )
