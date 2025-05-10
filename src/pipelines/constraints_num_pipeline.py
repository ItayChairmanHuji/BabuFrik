from src.action import Action
from src.pipelines.pipeline import Pipeline
from src.task import Task


class ConstraintsNumPipeline(Pipeline):
    def run_pipeline(self) -> None:
        for fds in self.fds:
            clean_task = Task(data=self.data, private_data_size=self.config.private_data_size,
                              synthetic_data_size=self.config.synthetic_data_size, fds=fds, action=Action.CLEANING)
            marginals_task = self.clean_data.submit(clean_task)
            synthesizing_task = self.get_marginals.submit(marginals_task.result(), wait_for=[marginals_task])
            for _ in range(self.config.generations_repeats):
                repairing_tasks = self.generate_synthetic_data.submit(synthesizing_task, wait_for=[synthesizing_task])
                for _ in range(self.config.repair_repeats):
                    self.repair_data.submit(repairing_tasks.result(), wait_for=[repairing_tasks])
