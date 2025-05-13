from dataclasses import dataclass

import wandb
from pandas import DataFrame
from wandb import Table
from wandb.apis.public import Run

from src.configuration import Configuration
from src.pipelines.run_type import RunType
from src.statistics import Statistics
from src.task import Task


@dataclass
class ResultsPublisher:
    config: Configuration
    run_type: RunType
    run: Run = None

    def __enter__(self) -> "ResultsPublisher":
        self.run = wandb.init(
            project="Private Synthetic Data Repair",
            entity="itay-chairman-hebrew-university-of-jerusalem",
            name=f"{self.config.dataset_name}_{self.config.generator_name}_{self.config.repair_algorithm}_{self.run_type}",
            config=self.config
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.run.finish()

    def publish_results(self, run: Run, task: Task, statistics: Statistics) -> None:
        n = 3
        measurements = ["runtime", "violations_count", "marginals_difference"]
        values = [statistics.runtime, statistics.violations_count, statistics.marginals_difference]
        if statistics.repair_size is not None:
            n = 4
            measurements.append("repair_size")
            values.append(statistics.repair_size)
        run.log({"results": Table(dataframe=DataFrame({
            "dataset": [self.config.dataset_name] * n,
            "synthesizer_algorithm": [self.config.generator_name] * n,
            "repair_algorithm": [self.config.repair_algorithm] * n,
            "private_data_size": [task.private_data_size] * n,
            "synthetic_data_size": [task.synthetic_data_size] * n,
            "number_of_constraints": [len(task.fds)] * n,
            "run_type": ["synthetic_data"] * n,
            "action": ["synthesizing"] * n,
            "measurement": measurements,
            "value": values,
        }))})
