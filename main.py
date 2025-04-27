import os
import sys
from pathlib import Path

from src.builders import task_builder
from src.running.task_configuration import TaskConfiguration
from src.storage import object_loader
from src.utils import consts


def load_task_configuration() -> tuple[str, TaskConfiguration]:
    task_config_file_name = sys.argv[1] if len(sys.argv) > 1 else consts.DEFAULT_TASK_CONFIGURATION_FILE_NAME
    task_config_file_path = str(os.path.join(consts.TASKS_CONFIGURATION_DIR_PATH, task_config_file_name))
    return Path(task_config_file_path).stem, TaskConfiguration(**object_loader.json_load(task_config_file_path))


def main():
    task_builder.build_task(
        *load_task_configuration()
    ).run_task()


if __name__ == '__main__':
    main()
