import inspect
import sys
from typing import Any

from src.statistics.statistics_analayzer import StatisticsAnalyzer


class StatisticsAnalyzersCreator:
    def create_statistics_analyzers(self, task_configuration: dict[str, Any]) -> list[StatisticsAnalyzer]:
        return [StatisticsAnalyzersCreator.create_statistics_analyzer(task_configuration, name, params)
                for name, params in task_configuration['statistics'].items()]

    def create_statistics_analyzer(self, task_configuration: dict[str, Any], name: str,
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
