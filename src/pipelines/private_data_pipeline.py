from src.entities.action import Action
from src.pipelines.pipeline import Pipeline
from src.entities.task import Task


class PrivateDataPipeline(Pipeline):
    def create_initial_tasks(self) -> list[Task]:
        return [Task(
            data=self.data,
            private_data_size=private_data_size,
            synthetic_data_size=self.config.synthetic_data_size,
            fds=self.fds,
            action=Action.CLEANING)
            for private_data_size in self.config.private_data_size]
