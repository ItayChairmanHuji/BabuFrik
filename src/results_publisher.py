import uuid
from dataclasses import replace, asdict, dataclass
from threading import Lock
from typing import Any

import wandb
from narwhals import DataFrame
from wandb import Table
from wandb.apis.public import Run

from src.configuration import Configuration
from src.statistics import Statistics
from src.task import Task


@dataclass
class ResultsPublisher:
    run_id: str
    config: Configuration
    lock: Lock = Lock()

    def publish_results(self, task: Task, statistics: Statistics) -> None:
        with self.lock:
            run = self.create_run(task)
            run.log({"results": Table(dataframe=DataFrame({
                "dataset": [self.config.dataset_name] * 3,
                "synthesizer_algorithm": [self.config.generator_name] * 3,
                "repair_algorithm": [self.config.repair_algorithm] * 3,
                "private_data_size": [task.private_data_size] * 3,
                "synthetic_data_size": [task.synthetic_data_size] * 3,
                "number_of_constraints": [len(task.fds)] * 3,
                "run_type": ["synthetic_data"] * 3,
                "action": ["synthesizing"] * 3,
                "measurement": ["runtime", "violations_count", "marginals_difference"],
                "value": [statistics.runtime, statistics.violations_count, statistics.marginals_difference],
            }))})
            run.finish()

    def create_run(self, task: Task) -> Run:
        return wandb.init(
            project="Private Synthetic Data Repair",
            entity="itay-chairman-hebrew-university-of-jerusalem",
            name=f"{self.config.dataset_name}_{task.action}",
            config=self.create_run_config(task),
            id=str(uuid.uuid4()),
            reinit=True
        )

    def create_run_config(self, task: Task) -> dict[str, Any]:
        config = replace(self.config, private_data_size=task.private_data_size,
                         synthetic_data_size=task.synthetic_data_size)
        config = asdict(config)
        config["run_id"] = self.run_id
        config["num_of_constraints"] = len(task.fds)
        return config
