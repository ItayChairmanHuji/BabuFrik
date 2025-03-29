import json
import os
from typing import Any, TypeVar

import wandb
from wandb.apis.public import Run

from src.analyzers.analyzer import Analyzer
from src.utils import class_loader, consts

AnalyzerSubClass = TypeVar("AnalyzerSubClass", bound=Analyzer)


def build_analyzer(analyzer_name: str, run: Run) -> Analyzer:
    config = __get_service_static_config(analyzer_name)
    analyzer_class = __load_analyzer_class(config)
    x_axis_name = config["x_axis_name"] if "x_axis_name" in config else config["x_axis"]
    return analyzer_class(
        run=run,
        table=wandb.Table(columns=[x_axis_name, analyzer_class.section()]),
        config=config
    )


def __get_service_static_config(analyzer_name: str) -> dict[str, Any]:
    config_file_path = os.path.join(consts.CONFIGURATION_DIR_PATH, "analyzers", f"{analyzer_name}.json")
    config = json.load(open(config_file_path, "r"))
    config["analyzer_name"] = analyzer_name
    if "vega_spec_name" not in config:
        config["vega_spec_name"] = "wandb/line/v0"
    return config


def __load_analyzer_class(config: dict[str, Any]) -> type[AnalyzerSubClass]:
    analyzer_class = class_loader.load_class(config["code_name"], kind="analyzers")
    if not issubclass(analyzer_class, Analyzer):
        raise Exception(f"The class {analyzer_class} is not an analyzer.")
    return analyzer_class
