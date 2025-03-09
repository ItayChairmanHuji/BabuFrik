import json
import os
import sys
import uuid

from src.running.job import Job
from src.running.task import Task
from src.running.task_configuration import TaskConfiguration
from src.utils import consts


def main():
    task_config_file_name = sys.argv[1] if len(sys.argv) > 1 else consts.DEFAULT_TASK_CONFIGURATION_FILE_NAME
    task_config_file_path = os.path.join(consts.TASKS_CONFIGURATION_DIR_PATH, task_config_file_name)
    task_config = TaskConfiguration(**json.load(open(task_config_file_path)))
    task_id = str(uuid.uuid4())
    working_dir = os.path.join(consts.TASKS_DIR_PATH, task_id)
    os.makedirs(working_dir, exist_ok=True)
    task = Task(working_dir=task_id,
                job=Job(
                    services=task_config.services,
                    fds_file_path=task_config.functional_dependencies_file_path,
                    marginals_errors_margins_file_path=task_config.marginals_errors_margins_file_path),
                dynamic_fields=task_config.dynamic_fields,
                analyzers_names=task_config.analyzers)
    task.run()


if __name__ == '__main__':
    main()
