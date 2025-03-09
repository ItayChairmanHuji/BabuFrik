import os
from dataclasses import dataclass
from typing import Any

from matplotlib import pyplot as plt

from src.analyzers import analyzers_utils
from src.analyzers.analyzer import Analyzer
from src.running.job import Job


@dataclass
class Task:
    working_dir: str
    job: Job
    dynamic_fields: dict[str, list[Any]]
    analyzers_names: list[str]

    def run(self) -> None:
        reports = []
        analyzers = self.__create_analyzers()
        for job_index, dynamic_values in enumerate(zip(*self.dynamic_fields)):
            job_working_dir = os.path.join(self.working_dir, str(job_index))
            os.makedirs(job_working_dir, exist_ok=True)
            dynamic_fields = {field: value for field, value in zip(self.dynamic_fields.keys(), dynamic_values)}
            reports += self.job.run(job_working_dir, dynamic_fields)
            for statistics_analyzer in analyzers:
                statistics_analyzer.analyze(reports)

    def __create_analyzers(self) -> list[Analyzer]:
        return [self.__create_analyzer(analyzer) for analyzer in self.analyzers_names]

    def __create_analyzer(self, analyzer_name: str) -> Analyzer:
        analyzer_class = analyzers_utils.load_analyzer_class(analyzer_name)
        return analyzer_class(
            working_dir=os.path.join(self.working_dir, "results"),
            figure=plt.figure(),
            config=analyzers_utils.load_analyzer_configuration(
                analyzer_name, analyzer_class.mandatory_fields(), self.dynamic_fields)
        )
