from src.entities.task import Task
from src.pipelines.pipeline import Pipeline


class ConstraintsNumPipeline(Pipeline):
    def create_initial_tasks(self) -> list[Task]:
        return [Task(
            private_data=self.data,
            private_data_size=self.config.private_data_size,
            synthetic_data_size=self.config.synthetic_data_size,
            fds=fds,
            relative_num_of_private_marginals=self.config.relative_num_of_marginals,
            marginals_privacy_budget=self.config.marginals_privacy_budget)
            for fds in self.fds]
