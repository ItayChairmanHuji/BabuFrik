from dataclasses import replace

from src.action import Action
from src.pipelines.pipeline import Pipeline
from src.task import Task


class PrivateDataPipeline(Pipeline):
    def run_task(self, task: Task) -> list[Task]:
        result_tasks = []
        match task.action:
            case Action.INITIALISING:
                result_tasks.append(replace(task, action=Action.CLEANING))
            case Action.CLEANING:
                for private_data_size in self.config.private_data_size:
                    result_tasks.append(self.clean_data.submit(replace(task, private_data_size=private_data_size)))
            case Action.MARGINALS:
                result_tasks.append(self.get_marginals.submit(task))
            case Action.SYNTHESIZING:
                for _ in range(self.config.generations_repeats):
                    result_tasks.append(self.generate_synthetic_data.submit(task))
            case Action.REPAIRING:
                pass
            case Action.TERMINATING:
                pass
            case _:
                raise NotImplementedError("Invalid action")
        return result_tasks
