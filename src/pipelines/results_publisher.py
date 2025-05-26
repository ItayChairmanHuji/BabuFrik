from dataclasses import replace, asdict, dataclass
from typing import Any

import wandb
from pandas import DataFrame
from wandb import Table
from wandb.apis.public import Run

from src.entities.configuration import Configuration
from src.entities.statistics import Statistics
from src.entities.task import Task


@dataclass
class ResultsPublisher:
    run_id: str
    config: Configuration

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

    def create_run(self, task: Task) -> Run:
        return wandb.init(
            project="Private Synthetic Data Repair",
            entity="itay-chairman-hebrew-university-of-jerusalem",
            name=f"{self.config.dataset_name}_{task.action}",
            config=self.create_run_config(task),
            settings=wandb.Settings(start_method="thread")
        )

    def create_run_config(self, task: Task) -> dict[str, Any]:
        config = replace(self.config, private_data_size=task.private_data_size,
                         synthetic_data_size=task.synthetic_data_size)
        config = asdict(config)
        config["run_id"] = self.run_id
        config["num_of_constraints"] = len(task.fds)
        return config
