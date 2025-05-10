from src.action import Action
from src.pipelines.pipeline import Pipeline
from src.task import Task


class PrivateDataPipeline(Pipeline):
    def run_pipeline(self) -> None:
        for private_data_size in self.config.private_data_size:
            marginals_task = self.clean_data.submit(Task(data=self.data, private_data_size=private_data_size,
                                                         synthetic_data_size=self.config.synthetic_data_size,
                                                         fds=self.fds, action=Action.CLEANING))
            synthesizing_task = self.get_marginals.submit(marginals_task.result(), wait_for=[marginals_task])
            for _ in range(self.config.generations_repeats):
                self.generate_synthetic_data.submit(synthesizing_task.result(), wait_for=[synthesizing_task])
