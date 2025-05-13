from abc import abstractmethod
from dataclasses import dataclass, replace
from typing import Union, Callable

import ray
from pandas import DataFrame

from src import utils
from src.action import Action
from src.actors import repairing, synthesizing, cleaning
from src.configuration import Configuration
from src.constraints.functional_dependencies import FunctionalDependencies
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.marginals_accessors import public_marginals_access
from src.results_publisher import ResultsPublisher
from src.task import Task


@dataclass
class Pipeline:
    run_id: str
    data: DataFrame
    config: Configuration
    fds: Union[FunctionalDependencies | list[FunctionalDependencies]]
    marginals_errors_margins: MarginalsErrorsMargins
    results_publisher: ResultsPublisher

    def run(self) -> None:
        with self.results_publisher:
            self.run_pipeline()

    @abstractmethod
    def run_pipeline(self):
        raise NotImplementedError("Not implemented run task method")

    @ray.remote
    def clean_data(self, task: Task) -> Task:
        clean_data = cleaning.clean_data(data=task.data,
                                         empty_values_threshold=self.config.empty_values_threshold,
                                         columns_threshold=self.config.columns_threshold,
                                         columns_to_keep=list(task.fds.attributes),
                                         rows_threshold=task.private_data_size,
                                         unique_values_threshold=self.config.unique_values_threshold)
        clean_data_size = min(task.private_data_size, len(clean_data))
        return replace(task, data=clean_data, private_data_size=clean_data_size, action=Action.MARGINALS)

    @ray.remote
    def get_marginals(self, task: Task) -> Task:
        return replace(task, action=Action.SYNTHESIZING, marginals=public_marginals_access.get_marginals(task.data))

    @ray.remote
    def generate_synthetic_data(self, task: Task) -> Task:
        func = lambda: synthesizing.generate_synthetic_data(data=task.data, training_epsilon=self.config.epsilon,
                                                            model_name=self.config.generator_name,
                                                            unique_values_threshold=self.config.unique_values_threshold,
                                                            model_extra_data=self.config.generator_extra_data,
                                                            sample_size=task.synthetic_data_size)
        return replace(task, data=self.run_and_publish(func, task), action=Action.REPAIRING)

    @ray.remote
    def repair_data(self, task: Task) -> Task:
        func = lambda: repairing.repair_data(data=task.data, fds=task.fds, marginals=task.marginals,
                                             marginals_error_margins=self.marginals_errors_margins,
                                             repair_algorithm=self.config.repair_algorithm)

        if self.config.should_calc_repair_size and task.action == Action.REPAIRING:
            optimal_repaired_size = len(repairing.repair_data(data=task.data, fds=task.fds, marginals=task.marginals,
                                                              marginals_error_margins=self.marginals_errors_margins,
                                                              repair_algorithm="ilp")) if self.config.repair_algorithm != "ilp" else -1
            return replace(task, data=self.run_and_publish(func, task, optimal_repaired_size))

        return replace(task, data=self.run_and_publish(func, task))

    def run_and_publish(self, func: Callable[[], DataFrame], task: Task, optimal_repair_size: int = None) -> DataFrame:
        with self.results_publisher.create_run(task) as run:
            result, statistics = utils.run_with_statistics(func, task.fds, task.marginals)
            if optimal_repair_size is not None:
                if optimal_repair_size == -1:
                    statistics.repair_size = 1
                elif optimal_repair_size == len(task.data):
                    statistics.repair_size = 1 if len(result) == optimal_repair_size \
                        else (len(task.data) - len(result)) / 0.00001
                else:
                    statistics.repair_size = float(len(task.data) - len(result)) / (
                            len(task.data) - optimal_repair_size)
            self.results_publisher.publish_results(run, task, statistics)
        return result
