import ray

from src.action import Action
from src.pipelines.pipeline import Pipeline
from src.task import Task


class ConstraintsNumPipeline(Pipeline):
    def run(self):
        results = []
        for fds in self.fds:
            clean_task = Task(data=self.data, private_data_size=self.config.private_data_size,
                              synthetic_data_size=self.config.synthetic_data_size, fds=fds, action=Action.CLEANING)
            marginals_task = self.clean_data.remote(self, clean_task)
            synthesizing_task = self.get_marginals.remote(self, marginals_task)
            for _ in range(self.config.generations_repeats):
                repairing_tasks = self.generate_synthetic_data.remote(self, synthesizing_task)
                if self.config.repair_repeats == 0:
                    results.append(repairing_tasks)
                for _ in range(self.config.repair_repeats):
                    results.append(self.repair_data.remote(self, repairing_tasks))
        ray.get(results)
