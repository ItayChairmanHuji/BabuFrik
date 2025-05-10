from dataclasses import replace

from src.action import Action
from src.pipelines.pipeline import Pipeline
from src.task import Task


class SyntheticDataPipeline(Pipeline):
    def run_task(self, task: Task) -> list[Task]:
        result_tasks = []
        match task.action:
            case Action.INITIALISING:
                result_tasks.append(replace(task, action=Action.CLEANING))
            case Action.CLEANING:
                result_tasks.append(self.clean_data.submit(task))
            case Action.MARGINALS:
                result_tasks.append(self.get_marginals.submit(task))
            case Action.SYNTHESIZING:
                for synthetic_data_size in self.config.synthetic_data_size:
                    for _ in range(self.config.generations_repeats):
                        result_tasks.append(
                            self.generate_synthetic_data.submit(replace(task, synthetic_data_size=synthetic_data_size)))
            case Action.REPAIRING:
                for _ in range(self.config.repair_repeats):
                    result_tasks.append(self.repair_data.submit(task))
            case Action.TERMINATING:
                pass
            case _:
                raise NotImplementedError("Invalid action")
        return result_tasks
