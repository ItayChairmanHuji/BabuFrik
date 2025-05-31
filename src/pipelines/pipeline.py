import time
from abc import abstractmethod, ABC
from dataclasses import dataclass, replace
from typing import Union, Callable

import ray
from pandas import DataFrame
from ray._raylet import ObjectRef

from src.actors import repairing, synthesizing, cleaning, quality_calculator
from src.constraints.functional_dependencies import FunctionalDependencies
from src.entities.action import Action
from src.entities.algorithms import QualityFunctions
from src.entities.configuration import Configuration
from src.entities.statistics import Statistics
from src.entities.task import Task
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.marginals_accessors import public_marginals_access
from src.pipelines.results_publisher import ResultsPublisher


@dataclass
class Pipeline(ABC):
    run_id: str
    data: DataFrame
    config: Configuration
    fds: Union[FunctionalDependencies | list[FunctionalDependencies]]
    marginals_errors_margins: MarginalsErrorsMargins
    results_publisher: ResultsPublisher

    def get_func_action(self, func: Callable[[Task], Task]) -> Action:
        if func == self.clean_data:
            return Action.CLEANING
        elif func == self.generate_synthetic_data:
            return Action.SYNTHESIZING
        elif func == self.get_marginals:
            return Action.MARGINALS
        elif func == self.repair_data:
            return Action.REPAIRING
        raise Exception("Invalid quality_function")

    def run(self) -> None:
        pending_tasks = []
        initial_tasks = self.create_initial_tasks()
        for initial_task in initial_tasks:
            clean_task_id = self.run_task.remote(self, initial_task, self.clean_data)
            synth_task_id = self.run_task.remote(self, clean_task_id, self.generate_synthetic_data)
            marginals_task_id = self.run_task.remote(self, synth_task_id, self.get_marginals)
            repairing_task_id = self.run_task.remote(self, marginals_task_id, self.repair_data)
            pending_tasks.append(repairing_task_id)
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
    def run_task(self, task: Task, task_func: Callable[[Task], Task]) -> Task:
        action = self.get_func_action(task_func)
        quality_function = self.config.quality_function(action)
        if quality_function == QualityFunctions.NO_QUALITY:
            return task_func(task)

        with self.results_publisher.create_run(task) as run:
            start_time = time.time()
            result = task_func(task)
            runtime = time.time() - start_time
            quality = self.calculate_quality(result, quality_function)
            statistics = Statistics(runtime=runtime, action=action, quality_func=quality_function, quality=quality)
            self.results_publisher.publish_results(run, task, statistics)
            return result

    def clean_data(self, task: Task) -> Task:
        clean_data = cleaning.clean_data(data=task.private_data,
                                         empty_values_threshold=self.config.empty_values_threshold,
                                         rows_threshold=task.private_data_size)
        clean_data_size = min(task.private_data_size, len(task.private_data))

        return replace(task, private_data=clean_data, private_data_size=clean_data_size)

    def generate_synthetic_data(self, task: Task) -> Task:
        synthetic_data = synthesizing.generate_synthetic_data(data=task.private_data, fds=task.fds,
                                                              eps=self.config.synthesizing_privacy_budget,
                                                              model_name=self.config.synthesizing_algorithm,
                                                              model_extra_data=self.config.synthesizer_extra_data,
                                                              sample_size=task.synthetic_data_size)
        return replace(task, synthetic_data=synthetic_data)

    def get_marginals(self, task: Task) -> Task:
        marginals = public_marginals_access.get_marginals(task.private_data)
        return replace(task, marginals=marginals)

    def repair_data(self, task: Task) -> Task:
        repaired_data = repairing.repair_data(data=task.synthetic_data, fds=task.fds, marginals=task.marginals,
                                              marginals_error_margins=self.marginals_errors_margins,
                                              repair_algorithm=self.config.repairing_algorithm)
        return replace(task, repaired_data=repaired_data)

    def calculate_quality(self, task: Task, quality_function: QualityFunctions) -> float:
        return quality_calculator.calculate_quality(task=task, quality_function=quality_function,
                                                    marginals_error_margins=self.marginals_errors_margins,
                                                    target_attribute=self.config.target_attribute)
