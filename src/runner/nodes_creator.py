import inspect
import json
import os
import sys
from abc import ABCMeta
from typing import Any

from src.utils import consts
from src.utils.node import Node


class NodesCreator:
    @staticmethod
    def create_nodes(nodes_names: list[str], nodes_type: str, fields_to_modify: dict[str, Any]) -> list[Node]:
        nodes_modules_names = [f"src.{nodes_type}.{node_name}" for node_name in nodes_names]
        nodes_classes = [NodesCreator.__import_module_class(module_name) for module_name in nodes_modules_names]
        nodes_configurations = [NodesCreator.__load_node_configuration(node_name) for node_name in nodes_names]
        NodesCreator.__update_fields_in_config(nodes_configurations, fields_to_modify)
        return [node_class(config) for node_class, config in zip(nodes_classes, nodes_configurations)]

    @staticmethod
    def __import_module_class(module_name: str) -> ABCMeta:
        __import__(module_name)
        module = sys.modules[module_name]
        is_class_in_module = lambda member: inspect.isclass(member) and member.__module__ == module_name
        class_name = inspect.getmembers(module, is_class_in_module)[0][0]
        return getattr(module, class_name)

    @staticmethod
    def __load_node_configuration(node_name: str) -> dict[str, Any]:
        return json.load(open(os.path.join(consts.NODES_DIR_PATH, f"{node_name}.json"), "r"))

    @staticmethod
    def __update_fields_in_config(nodes_configurations: list[dict[str, Any]], fields_to_modify: dict[str, Any]) -> None:
        for node_configuration in nodes_configurations:
            fields_to_modify_in_node = list(node_configuration.keys() & fields_to_modify.keys())
            for field in fields_to_modify_in_node:
                node_configuration[field] = fields_to_modify[field]
