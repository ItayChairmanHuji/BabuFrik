from dataclasses import replace

from src.action import Action
from src.pipelines.pipeline import Pipeline
from src.task import Task


class SyntheticDataPipeline(Pipeline):
    def run_pipeline(self) -> None:
        clean_task = Task(data=self.data, private_data_size=self.config.private_data_size, synthetic_data_size=-1,
                          fds=self.fds, action=Action.CLEANING)
        marginals_task = self.clean_data.submit(clean_task)
        synthesizing_task = self.get_marginals.submit(marginals_task.result(), wait_for=[marginals_task])
        for synthetic_data_size in self.config.synthetic_data_size:
            for _ in range(self.config.generations_repeats):
                repairing_tasks = self.generate_synthetic_data.submit(
                    replace(synthesizing_task.result(), synthetic_data_size=synthetic_data_size),
                    wait_for=[synthesizing_task])
                for _ in range(self.config.repair_repeats):
                    self.repair_data.submit(repairing_tasks.result(), wait_for=[repairing_tasks])
