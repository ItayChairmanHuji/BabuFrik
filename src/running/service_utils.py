import json
import os
from typing import TypeVar, Any

from src.running.service import Service
from src.utils import class_loader, consts
from src.utils.configuration import Configuration

ServiceSubClass = TypeVar("ServiceSubClass", bound=Service)


def load_service(service_config_name: str,
                 dynamic_fields: dict[str, Any]) -> tuple[type[ServiceSubClass], Configuration]:
    service_type, config_name = service_config_name.split(".")
    service_config = __get_json_config(service_type, config_name)
    service_class = __get_service_class(service_config, service_type)
    return service_class, __get_service_configuration(service_config, service_class, dynamic_fields)


def __get_json_config(service_type: str, config_name: str) -> dict[str, Any]:
    config_file_path = os.path.join(consts.CONFIGURATION_DIR_PATH, service_type, f"{config_name}.json")
    return json.load(open(config_file_path, "r"))


def __get_service_class(service_config: dict[str, Any], service_type: str) -> type[ServiceSubClass]:
    service_name = service_config["code_name"]
    service_class = class_loader.load_class(service_name, service_type)
    if not issubclass(service_class, Service):
        raise Exception(f"Service class {service_name} not found.")
    return service_class


def __get_service_configuration(service_config: dict[str, Any],
                                service_class: type[ServiceSubClass],
                                dynamic_fields: dict[str, list[Any]]) -> Configuration:
    mandatory_fields = service_class.mandatory_fields()
    for field, value in dynamic_fields.items():
        if field in mandatory_fields:
            service_config[field] = value
    return Configuration(service_config, mandatory_fields)
