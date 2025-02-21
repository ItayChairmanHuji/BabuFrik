import os
import shutil
import time
import uuid
from itertools import chain
from typing import Any, TypeVar

from src.runner.nodes_creator import NodesCreator
from src.utils import consts
from src.utils.node import Node

T = TypeVar("T", bound=Node)


def create_working_dir_path() -> str:
    task_id = str(uuid.uuid4())
    return os.path.join(consts.TASKS_DIR_PATH, task_id)


class RouteRunner:
    NODES_TYPES = ["generators", "cleaners", "synthesizers", "repairers"]

    def __init__(self):
        self.working_dir = None
        self.times: dict[str, float] = {}

    @property
    def task_id(self) -> str:
        return os.path.basename(self.working_dir)

    def run(self, route_config: dict[str, Any], **kwargs) -> None:
        nodes = self.__create_all_nodes(route_config, kwargs)
        self.__setup_working_dir(route_config)
        self.__runner_loop(nodes)

    def __create_all_nodes(self, route_config: dict[str, list[str]], fields_to_modify: dict[str, Any]) -> list[Node]:
        create_nodes = lambda node_type: NodesCreator.create_nodes(route_config[node_type], node_type, fields_to_modify)
        return chain.from_iterable(create_nodes(node_type) for node_type in self.NODES_TYPES)

    def __setup_working_dir(self, route_config: dict[str, str]) -> None:
        self.working_dir = create_working_dir_path()
        os.makedirs(self.working_dir, exist_ok=True)
        shutil.copyfile(
            os.path.join(consts.FUNCTIONAL_DEPENDENCIES_DIR_PATH, route_config["functional_dependencies_file_name"]),
            os.path.join(self.working_dir, consts.FUNCTIONAL_DEPENDENCIES_FILE_NAME))

    def __runner_loop(self, nodes: list[Node]) -> None:
        input_file_for_node = None
        print(f"Running task {self.task_id}")
        for node in nodes:
            self.__run_node(node, input_file_for_node)
            input_file_for_node = node.output_file_path()

    def __run_node(self, node: Node, input_file_path: str):
        print(f"Running node {node.name()}")
        start_time = time.time()
        node.run(self.working_dir, input_file_path)
        end_time = time.time()
        self.times[node.name()] = end_time - start_time
