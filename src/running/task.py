import os
from dataclasses import dataclass

from src.running.job import Job
from src.utils.message import Message


@dataclass
class Task:
    working_dir: str
    fds_file_path: str
    marginals_errors_margins_file_path: str
    jobs: list[Job]

    def run(self) -> None:
        messages = [Message(self.working_dir)]
        for job in self.jobs:
            reports = []
            for message in messages:
                reports += job.run(message)
            messages = [Message(
                working_dir=os.path.dirname(report.output_file_path),
                input_file=report.input_file_path)
                for report in reports]

