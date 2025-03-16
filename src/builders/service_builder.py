import json
import os
from typing import TypeVar, Any

from wandb.apis.public import Run

from src.running.service import Service
from src.running.task_configuration import TaskConfiguration
from src.utils import class_loader, consts
from src.utils.configuration import Configuration
from src.utils.temp_dir import TempDir

ServiceSubClass = TypeVar("ServiceSubClass", bound=Service)


def build_service(service_config_name: str, task_config: TaskConfiguration, run: Run) -> Service:
    config = __get_service_static_config(service_config_name)
    service_class = __load_service_class(config)
    if "information_to_publish" in config:
        __update_run_configuration(run, config["information_to_publish"])
    return service_class(
        working_dir=TempDir(base_path=task_config.working_dir),
        fds_file_path=task_config.functional_dependencies_file_name,
        marginals_errors_margins_file_path=task_config.marginals_errors_margins_file_name,
        config=Configuration(config, service_class.mandatory_fields())
    )


def __get_service_static_config(service_config_name: str) -> dict[str, Any]:
    service_type, config_name = service_config_name.split(".")
    config_file_path = os.path.join(consts.CONFIGURATION_DIR_PATH, service_type, f"{config_name}.json")
    static_config = json.load(open(config_file_path, "r"))
    static_config["config_name"] = config_name
    static_config["service_type"] = service_type
    if "dynamic_fields" not in static_config:
        static_config["dynamic_fields"] = {"": [""]}
    if "analyzers" not in static_config:
        static_config["analyzers"] = []
    return static_config


def __load_service_class(config: dict[str, Any]) -> type[ServiceSubClass]:
    service_class = class_loader.load_class(config["code_name"], config["service_type"])
    if not issubclass(service_class, Service):
        raise Exception(f"The class {service_class} is not a service.")
    return service_class


def __update_run_configuration(run: Run, information_to_publish: dict[str, Any]) -> None:
    for key, value in information_to_publish.items():
        run.config[key] = value
