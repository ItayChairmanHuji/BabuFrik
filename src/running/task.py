import os
import shutil
from dataclasses import dataclass

import pandas as pd
from wandb.apis.public import Run
from wandb.sdk.data_types.table import Table

from src.running.job import Job
from src.utils import consts
from src.utils.message import Message


@dataclass
class Task:
    working_dir: str
    fds_file_path: str
    marginals_errors_margins_file_path: str
    jobs: dict[str, Job]
    routing: dict[str, str]
    run: Run

    def run_task(self) -> None:
        print(f"Running task {self.task_id}")
        initial_extra_data = {
            "fds_file_path": self.fds_file_path,
            "marginals_error_margins_file_path": self.marginals_errors_margins_file_path
        }
        messages = [Message(extra_data=initial_extra_data, to_service=self.routing[""])]
        while len(messages) > 0:
            message = messages.pop()
            if message.to_service is None:
                continue
            messages += [self.__route_message(m) for m in self.jobs[message.to_service].run(message)]

        results_dir = os.path.join(self.working_dir, consts.RESULTS_DIR_NAME)
        results = pd.concat([pd.read_csv(os.path.join(results_dir, result_file)) for result_file in os.listdir(results_dir)])
        self.run.log({"results": Table(data=results)})
        shutil.rmtree(self.working_dir)

    @property
    def task_id(self) -> str:
        return os.path.basename(self.working_dir)

    def __route_message(self, message: Message) -> Message:
        message.to_service = self.routing[message.from_service]
        return message
