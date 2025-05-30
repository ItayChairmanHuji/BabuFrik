import time
from abc import abstractmethod, ABC
from dataclasses import dataclass

from pandas import DataFrame

from src.entities.algorithms import NodesTypes
from src.entities.configuration import Configuration
from src.entities.task import Task
from src.pipelines.results_publisher import ResultsPublisher
from src.quality_functions.quality_function import QualityFunction


@dataclass
class Node(ABC):
    config: Configuration
    quality_function: QualityFunction
    results_publisher: ResultsPublisher


    def run(self, task: Task) -> DataFrame:
        if self.config.monitored_object != self.node_type:
            return self.node_action(task)

        with self.results_publisher.create_run(task) as run:
            result, statistics = utils.utils.run_with_statistics(func, task.fds, task.marginals)

            self.results_publisher.publish_results(run, task, statistics)
        return result

    def run_with_statistics(self, task: Task) -> DataFrame:
        with self.results_publisher.create_run(task) as run:
            start_time = time.time()
            result = self.node_action(task)
            runtime = time.time() - start_time
            quality = self.quality_function.calculate_quality()
            self.results_publisher.publish_results(run, task, statistics)


    @abstractmethod
    def node_action(self, task: Task) -> DataFrame:
        raise NotImplementedError()

    @property
    @abstractmethod
    def node_type(self) -> NodesTypes:
        raise NotImplementedError()
