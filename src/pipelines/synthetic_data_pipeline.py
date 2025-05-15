from src.action import Action
from src.pipelines.pipeline import Pipeline
from src.task import Task


class SyntheticDataPipeline(Pipeline):
    def create_initial_tasks(self) -> list[Task]:
        return [Task(
            data=self.data,
            private_data_size=self.config.private_data_size,
            synthetic_data_size=synthetic_data_size,
            fds=self.fds,
            action=Action.CLEANING)
            for synthetic_data_size in self.config.synthetic_data_size]
