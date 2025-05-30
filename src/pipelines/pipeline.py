import time
from abc import abstractmethod, ABC
from dataclasses import dataclass, replace
from typing import Union

import ray
from pandas import DataFrame
from ray._raylet import ObjectRef

from src.actors import repairing, synthesizing, cleaning
from src.constraints.functional_dependencies import FunctionalDependencies
from src.entities.action import Action
from src.entities.configuration import Configuration
from src.entities.task import Task
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.marginals_accessors import public_marginals_access
from src.pipelines.results_publisher import ResultsPublisher
from src.quality_functions.quality_function import QualityFunction


@dataclass
class Pipeline(ABC):
    run_id: str
    data: DataFrame
    config: Configuration
    fds: Union[FunctionalDependencies | list[FunctionalDependencies]]
    marginals_errors_margins: MarginalsErrorsMargins
    quality_function: QualityFunction
    results_publisher: ResultsPublisher

    def run(self) -> None:
        pending_tasks = []
        initial_tasks = self.create_initial_tasks()
        for initial_task in initial_tasks:
            with self.results_publisher.create_run(initial_task) as run:
                start_time = time.time()
                cleaned_data = self.clean_data.remote(self, initial_task)
                cleaning_time = time.time() - start_time
                marginals = self.get_marginals.remote(self, cleaned_data)
                marginals_time = time.time() - cleaning_time
                synthetic_data = self.generate_synthetic_data.remote(self, marginals)
                synthetic_data_time = time.time() - marginals_time
                repaired_data = self.repair_data.remote(self, synthetic_data)
                repairing_time = time.time() - synthetic_data_time
                task = self.calculate_quality.remote(self, cleaned_data, synthetic_data, repaired_data)
                pending_tasks.append(task)
                pending_tasks = self.wait_for_pending_tasks(pending_tasks)
        self.finish_last_pending_tasks(pending_tasks)

    def wait_for_pending_tasks(self, pending_tasks: list[ObjectRef]) -> list[ObjectRef]:
        if len(pending_tasks) >= self.config.num_of_tasks_in_parallel:
            ready_tasks, pending_tasks = ray.wait(pending_tasks, num_returns=1)
            ray.get(ready_tasks)
        return pending_tasks

    @staticmethod
    def finish_last_pending_tasks(pending_tasks: list) -> None:
        if pending_tasks:
            ray.get(pending_tasks)

    @abstractmethod
    def create_initial_tasks(self) -> list[Task]:
        raise NotImplementedError("Not implemented")

    @ray.remote
    def clean_data(self, task: Task) -> Task:
        clean_data = cleaning.clean_data(data=task.data,
                                         empty_values_threshold=self.config.empty_values_threshold,
                                         rows_threshold=task.private_data_size)
        clean_data_size = min(task.private_data_size, len(clean_data))
        return replace(task, data=clean_data, private_data_size=clean_data_size, action=Action.MARGINALS)

    # TODO: Implement marginals generation algorithm
    @ray.remote
    def get_marginals(self, task: Task) -> Task:
        return replace(task, action=Action.SYNTHESIZING, marginals=public_marginals_access.get_marginals(task.data))

    @ray.remote
    def generate_synthetic_data(self, task: Task) -> Task:
        synthetic_data = synthesizing.generate_synthetic_data(data=task.data, fds=task.fds,
                                                              eps=self.config.synthesizing_privacy_budget,
                                                              model_name=self.config.synthesizing_algorithm,
                                                              model_extra_data=self.config.synthesizer_extra_data,
                                                              sample_size=task.synthetic_data_size)
        return replace(task, data=synthetic_data, action=Action.REPAIRING)

    @ray.remote
    def repair_data(self, task: Task) -> Task:
        repaired_data = repairing.repair_data(data=task.data, fds=task.fds, marginals=task.marginals,
                                              marginals_error_margins=self.marginals_errors_margins,
                                              repair_algorithm=self.config.repairing_algorithm)
        return replace(task, data=repaired_data)

    @ray.remote
    def calculate_quality(self, clean_task: Task, syn_task: Task, repair_task: Task) -> None:
        self.quality_function.calculate_quality(private_dataset=clean_task.data,
                                                synthetic_dataset=syn_task.data,
                                                repaired_dataset=repair_task.data,
                                                marginals=repair_task.marginals,
                                                fds=repair_task.fds,
                                                marginals_error_margins=self.marginals_errors_margins,
                                                target_attribute=self.config.target_attribute)
        # self.results_publisher.publish_results(run, task, statistics)
