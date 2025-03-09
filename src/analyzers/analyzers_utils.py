import json
import os
from typing import TypeVar, Any

from src.analyzers.analyzer import Analyzer
from src.utils import class_loader, consts
from src.utils.configuration import Configuration

AnalyzerSubClass = TypeVar("AnalyzerSubClass", bound=Analyzer)


def load_analyzer_class(analyzer_name: str) -> type[AnalyzerSubClass]:
    analyzer_class = class_loader.load_class(analyzer_name)
    if not issubclass(analyzer_class, Analyzer):
        raise Exception(f"Analyzer class {analyzer_name} not found.")
    return analyzer_class


def is_analyzer_type(analyzer_name: str) -> bool:
    return analyzer_name.split('_')[-1] == "analyzer"


def load_analyzer_configuration(analyzer_name: str, mandatory_fields: list[str],
                                dynamic_fields: dict[str, list[Any]]) -> Configuration:
    if not is_analyzer_type(analyzer_name):
        raise TypeError(f"Analyzer class {analyzer_name} not found.")

    config_file_path = os.path.join(consts.CONFIGURATION_DIR_PATH, f"analyzers", f"{analyzer_name}.json")
    config = json.load(open(config_file_path, "r"))
    if "x_axis_label" in mandatory_fields:
        x_axis_label = config["x_axis_label"]
        config["x_axis"] = dynamic_fields[x_axis_label]
    return Configuration(config, mandatory_fields)
