import inspect
import json
import os
import sys
import uuid
from typing import Any

from src.runner.route_runner import RouteRunner
from src.statistics.statistics_analayzer import StatisticsAnalyzer
from src.utils import consts


def main():
    task_id = str(uuid.uuid4())
    running_config_file = consts.MAIN_CONFIGURATION_FILE_PATH  ## Change
    running_config = json.load(open(running_config_file))
    working_dir = os.path.join(consts.TASKS_DIR_PATH, task_id)
    os.makedirs(working_dir, exist_ok=True)
    runner = RouteRunner(working_dir)
    statistics_analyzers = create_statistics_analyzers(running_config)
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
        for statistics_analyzer in statistics_analyzers:
            statistics_analyzer.analyze_statistics(reports)


def create_statistics_analyzers(task_configuration: dict[str, Any]) -> list[StatisticsAnalyzer]:
    return [create_statistics_analyzer(task_configuration, name, params)
            for name, params in task_configuration['statistics'].items()]


def create_statistics_analyzer(task_configuration: dict[str, Any], name: str,
                               params: dict[str, Any]) -> StatisticsAnalyzer:
    module_name = f"src.statistics.{name}"
    __import__(module_name)
    module = sys.modules[module_name]
    is_class_in_module = lambda member: inspect.isclass(member) and member.__module__ == module_name
    class_name = inspect.getmembers(module, is_class_in_module)[0][0]
    class_type = getattr(module, class_name)
    if "x_axis" not in params:
        params["x_axis"] = task_configuration["fields_to_change"][params["x_axis_name"]]
    return class_type(**params)


if __name__ == '__main__':
    main()
