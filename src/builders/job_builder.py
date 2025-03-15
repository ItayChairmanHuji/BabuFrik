from wandb.apis.public import Run

from src.builders import service_builder, analyzer_builder
from src.running.job import Job
from src.running.task_configuration import TaskConfiguration


def build_job(service_name: str, task_config: TaskConfiguration, run: Run) -> Job:
    service = service_builder.build_service(service_name, task_config)
    analyzers = [analyzer_builder.build_analyzer(analyzer_name, service, run) for analyzer_name in service.analyzers]
    return Job(service, analyzers, service.dynamic_fields)
