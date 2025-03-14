import os
from collections import defaultdict
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
    analyzers_names: list[str]

    def run(self) -> None:
        reports = defaultdict(list)
        analyzers = self.__create_analyzers()
        for job_index, dynamic_values in enumerate(zip(*self.dynamic_fields.values())):
            print(f"Running job {job_index}")
            job_working_dir = os.path.join(self.working_dir, str(job_index))
            os.makedirs(job_working_dir, exist_ok=True)
            dynamic_fields = {field: value for field, value in zip(self.dynamic_fields.keys(), dynamic_values)}
            for report in self.job.run(job_working_dir, dynamic_fields):
                reports[report.service_name].append(report)
            for statistics_analyzer in analyzers:
                statistics_analyzer.analyze(reports)

    def __create_analyzers(self) -> list[Analyzer]:
        return [self.__create_analyzer(analyzer) for analyzer in self.analyzers_names]

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
