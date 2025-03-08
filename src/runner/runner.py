import json
import os
import uuid
from collections import defaultdict
from typing import Any

import matplotlib.pyplot as plt

from src.runner.route_runner import RouteRunner
from src.utils import consts


def main():
    task_id = str(uuid.uuid4())
    running_config_file = consts.MAIN_CONFIGURATION_FILE_PATH  ## Change
    running_config = json.load(open(running_config_file))
    working_dir = os.path.join(consts.TASKS_DIR_PATH, task_id)
    os.makedirs(working_dir, exist_ok=True)
    runner = RouteRunner(working_dir)
    route_name = running_config["route_name"]
    fields_to_modify: dict[str, list[Any]] = running_config["fields_to_modify"]
    reports = []
    print(f"Running task {task_id}")
    for index, values_to_modify in enumerate(zip(*fields_to_modify.values())):
        dynamic_fields = {key: value for key, value in zip(fields_to_modify.keys(), values_to_modify)}
        route_config = json.load(open(os.path.join(consts.ROUTES_DIR_PATH, f"{route_name}.json")))
        run_working_dir = os.path.join(working_dir, str(index))
        os.makedirs(run_working_dir, exist_ok=True)
        reports += runner.run(run_working_dir, route_config, **dynamic_fields)


if __name__ == '__main__':
    main()
