import json
import os
from typing import TypeVar, Any

from src.analyzers.analyzer import Analyzer
from src.utils import class_loader, consts
from src.utils.configuration import Configuration

AnalyzerSubClass = TypeVar("AnalyzerSubClass", bound=Analyzer)


def load_analyzer(analyzer_config_name: str, dynamic_fields: dict[str, list[Any]]) -> tuple[
    type[AnalyzerSubClass], Configuration]:
    analyzer_config = __get_json_config(analyzer_config_name)
    analyzer_class = __get_analyzer_class(analyzer_config)
    return analyzer_class, __get_analyzer_configuration(analyzer_config, analyzer_class, dynamic_fields)


def __get_json_config(analyzer_config_name: str) -> dict[str, Any]:
    config_file_path = os.path.join(consts.CONFIGURATION_DIR_PATH, "analyzers", f"{analyzer_config_name}.json")
    return json.load(open(config_file_path, "r"))


def __get_analyzer_class(analyzer_config: dict[str, Any]) -> type[AnalyzerSubClass]:
    analyzer_name = analyzer_config["code_name"]
    service_class = class_loader.load_class(analyzer_name, "analyzers")
    if not issubclass(service_class, Analyzer):
        raise Exception(f"Analyzer class {analyzer_name} not found.")
    return service_class


def __get_analyzer_configuration(analyzer_config: dict[str, Any],
                                 analyzer_class: type[AnalyzerSubClass],
                                 dynamic_fields: dict[str, list[Any]]) -> Configuration:
    mandatory_fields = analyzer_class.mandatory_fields()
    if "x_axis_label" in mandatory_fields:
        x_axis_label = analyzer_config["x_axis_label"]
        analyzer_config["x_axis"] = dynamic_fields[x_axis_label]
    return Configuration(analyzer_config, mandatory_fields)
