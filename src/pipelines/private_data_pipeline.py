import ray

from src.action import Action
from src.pipelines.pipeline import Pipeline
from src.task import Task


class PrivateDataPipeline(Pipeline):
    def run(self) -> None:
        results = []
        for private_data_size in self.config.private_data_size:
            marginals_task = self.clean_data.remote(self,  Task(data=self.data, private_data_size=private_data_size,
                                                               synthetic_data_size=self.config.synthetic_data_size,
                                                               fds=self.fds, action=Action.CLEANING))
            synthesizing_task = self.get_marginals.remote(self, marginals_task)
            for _ in range(self.config.generations_repeats):
                results.append(self.generate_synthetic_data.remote(self, synthesizing_task))
        print(results)
        ray.wait(results)
