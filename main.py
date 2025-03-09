import json
import os
import sys
import uuid

from src.running.task import Task
from src.running.task_configuration import TaskConfiguration
from src.utils import consts


def main():
    task_config_file = sys.argv[1] if len(sys.argv) > 1 else consts.MAIN_CONFIGURATION_FILE_PATH
    task_config = TaskConfiguration(**json.load(open(task_config_file)))
    task_id = str(uuid.uuid4())
    working_dir = os.path.join(consts.TASKS_DIR_PATH, task_id)
    os.makedirs(working_dir, exist_ok=True)
    task = Task(working_dir=task_id, task_configuration=task_config)
    task.run()


if __name__ == '__main__':
    main()
