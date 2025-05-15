from src.action import Action
from src.pipelines.pipeline import Pipeline
from src.task import Task


class ConstraintsNumPipeline(Pipeline):
    def create_initial_tasks(self) -> list[Task]:
        return [Task(
            data=self.data,
            private_data_size=self.config.private_data_size,
            synthetic_data_size=self.config.synthetic_data_size,
            fds=fds,
            action=Action.CLEANING)
            for fds in self.fds]
