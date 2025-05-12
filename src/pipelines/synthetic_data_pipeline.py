from src.action import Action
from src.pipelines.pipeline import Pipeline
from src.task import Task


class SyntheticDataPipeline(Pipeline):
    def run(self) -> None:
        results = []
        for synthetic_data_size in self.config.synthetic_data_size:
            clean_task = Task(data=self.data, private_data_size=self.config.private_data_size,
                              synthetic_data_size=synthetic_data_size, fds=self.fds, action=Action.CLEANING)
            marginals_task = self.clean_data.remote(self, clean_task)
            synthesizing_task = self.get_marginals.remote(self, marginals_task)
            for _ in range(self.config.generations_repeats):
                repairing_tasks = self.generate_synthetic_data.remote(self, synthesizing_task)
                for _ in range(self.config.repair_repeats):
                    results.append(self.repair_data.remote(self, repairing_tasks))
