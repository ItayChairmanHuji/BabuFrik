from abc import abstractmethod
from dataclasses import dataclass, replace, asdict
from typing import Union, Callable

import prefect
import wandb
from pandas import DataFrame
from prefect import flow
from prefect.futures import PrefectFuture
from wandb import Table
from wandb.apis.public import Run

from src import utils
from src.action import Action
from src.actors import repairing, synthesizing, cleaning
from src.configuration import Configuration
from src.constraints.functional_dependencies import FunctionalDependencies
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.marginals_accessors import public_marginals_access
from src.statistics import Statistics
from src.task import Task


@dataclass
class Pipeline:
    run_id: str
    data: DataFrame
    config: Configuration
    fds: Union[FunctionalDependencies | list[FunctionalDependencies]]
    marginals_errors_margins: MarginalsErrorsMargins
    results: DataFrame = DataFrame(columns=["dataset", "synthesizer_algorithm", "repair_algorithm",
                                            "private_data_size", "synthetic_data_size", "number_of_violations",
                                            "action", "value"])

    @flow
    def run(self) -> None:
        init_task = Task(data=self.data, private_data_size=-1,
                         synthetic_data_size=-1, fds=self.fds, action=Action.INITIALISING)
        ready_tasks = [init_task]
        working_tasks = []
        while len(ready_tasks) > 0 or len(working_tasks) > 0:
            while len(ready_tasks) > 0:
                task = ready_tasks.pop()
                working_tasks.extend(self.run_task(task))
            ready_tasks.extend([task for task in working_tasks if task.done()])
            working_tasks = [task for task in working_tasks if not task.done()]

    @abstractmethod
    def run_task(self, task: Task) -> list[PrefectFuture[Task]]:
        raise NotImplementedError("Not implemented run task method")

    @prefect.task
    def clean_data(self, task: Task) -> Task:
        clean_data = cleaning.clean_data(data=task.data, empty_values_threshold=self.config.empty_values_threshold,
                                         columns_threshold=self.config.columns_threshold,
                                         columns_to_keep=list(task.fds.attributes),
                                         rows_threshold=task.private_data_size,
                                         unique_values_threshold=self.config.unique_values_threshold)
        clean_data_size = min(task.private_data_size, len(clean_data))
        return replace(task, data=clean_data, private_data_size=clean_data_size, action=Action.MARGINALS)

    @prefect.task
    def get_marginals(self, task: Task) -> Task:
        return replace(task, action=Action.SYNTHESIZING, marginals=public_marginals_access.get_marginals(task.data))

    @prefect.task
    def generate_synthetic_data(self, task: Task) -> Task:
        func = lambda: synthesizing.generate_synthetic_data(data=task.data, training_epsilon=self.config.epsilon,
                                                            model_name=self.config.generator_name,
                                                            unique_values_threshold=self.config.unique_values_threshold,
                                                            model_extra_data=self.config.generator_extra_data,
                                                            sample_size=task.synthetic_data_size)
        return replace(task, data=self.run_and_publish(func, task), action=Action.REPAIRING)

    @prefect.task
    def repair_data(self, task: Task) -> Task:
        func = lambda: repairing.repair_data(data=task.data, fds=task.fds, marginals=task.marginals,
                                             marginals_error_margins=self.marginals_errors_margins,
                                             repair_algorithm=self.config.repair_algorithm)
        return replace(task, data=self.run_and_publish(func, task), action=Action.TERMINATING)

    def run_and_publish(self, func: Callable[[], DataFrame], task: Task) -> DataFrame:
        with self.create_run(task) as run:
            result, statistics = utils.run_with_statistics(func, task.fds, task.marginals)
            self.publish_statistics(run, task, statistics)
        return result

    def create_run(self, task: Task) -> Run:
        config = replace(self.config, private_data_size=task.private_data_size,
                         synthetic_data_size=task.synthetic_data_size)
        config = asdict(config)
        config["run_id"] = self.run_id
        config["num_of_constraints"] = len(task.fds)
        del config["fds"]
        return wandb.init(
            project="Private Synthetic Data Repair",
            entity="itay-chairman-hebrew-university-of-jerusalem",
            name=f"{self.config.dataset_name}_{task.action}",
            config=config
        )

    def publish_statistics(self, run: Run, task: Task, statistics: Statistics) -> None:
        run.log(Table({
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
        }))
