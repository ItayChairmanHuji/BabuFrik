import shutil
from dataclasses import dataclass

from src.running.job import Job
from src.utils.message import Message


@dataclass
class Task:
    working_dir: str
    fds_file_path: str
    marginals_errors_margins_file_path: str
    jobs: dict[str, Job]
    routing: dict[str, str]

    def run(self) -> None:
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
        shutil.rmtree(self.working_dir)

    def __route_message(self, message: Message) -> Message:
        message.to_service = self.routing[message.from_service]
        return message
