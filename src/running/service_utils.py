import json
import os
from typing import TypeVar, Any

from src.running.service import Service
from src.utils import class_loader, consts
from src.utils.configuration import Configuration

ServiceSubClass = TypeVar("ServiceSubClass", bound=Service)


def load_service_class(service_name: str) -> type[ServiceSubClass]:
    service_class = class_loader.load_class(service_name)
    if not issubclass(service_class, Service):
        raise Exception(f"Service class {service_name} not found.")
    return service_class


def get_service_type(service_name: str) -> str:
    return service_name.split('_')[-1]


def load_service_configuration(service_name: str, mandatory_fields: list[str],
                               dynamic_fields: dict[str, Any]) -> Configuration:
    service_type = get_service_type(service_name)
    config_file_path = os.path.join(consts.CONFIGURATION_DIR_PATH, f"{service_type}s", f"{service_name}.json")
    service_config = json.load(open(config_file_path, "r"))
    for field, value in dynamic_fields.items():
        if field in mandatory_fields:
            service_config[field] = value
    return Configuration(service_config, mandatory_fields)
