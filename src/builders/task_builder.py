import os
import uuid

import wandb
from wandb.apis.public import Run

from src.builders import job_builder
from src.running.task import Task
from src.running.task_configuration import TaskConfiguration
from src.utils import consts


def build_task(task_config: TaskConfiguration) -> Task:
    task_id = str(uuid.uuid4())
    task_config.working_dir = __create_working_dir(task_id, task_config)
    task_config.functional_dependencies_file_name = __get_fd_path(task_config)
    task_config.marginals_errors_margins_file_name = __get_marginals_path(task_config)
    run = __init_run(task_id, task_config)
    jobs = {job.service.name: job for service in task_config.services
            if (job := job_builder.build_job(service, task_config, run))}
    services_names = list(jobs.keys())
    routing = {services_names[i]: services_names[i + 1] for i in range(len(jobs) - 1)}
    routing[""] = services_names[0]
    routing[services_names[-1]] = None
    return Task(task_config.working_dir,
                task_config.functional_dependencies_file_name,
                task_config.marginals_errors_margins_file_name,
                jobs, routing)


def __init_run(task_id: str, task_config: TaskConfiguration) -> Run:
    api_key_file_name = task_config.results_dashboard_api_key_file_name
    api_key_file_path = os.path.join(consts.LICENSES_DIR_PATH, api_key_file_name)
    api_key = open(api_key_file_path).read()
    os.environ["WANDB_SILENT"] = "True"
    wandb.login(key=api_key)
    return wandb.init(project=task_config.results_dashboard_project_name, name=task_id)


def __create_working_dir(task_id: str, task_config: TaskConfiguration) -> str:
    if task_config.working_dir is not None:
        return task_config.working_dir
    working_dir = os.path.join(consts.TASKS_DIR_PATH, task_id)
    os.makedirs(working_dir, exist_ok=True)
    return working_dir


def __get_fd_path(task_config: TaskConfiguration) -> str:
    return os.path.join(consts.FUNCTIONAL_DEPENDENCIES_DIR_PATH, task_config.functional_dependencies_file_name)


def __get_marginals_path(task_config: TaskConfiguration) -> str:
    return os.path.join(consts.MARGINALS_ERRORS_MARGINS_DIR_PATH, task_config.marginals_errors_margins_file_name)
