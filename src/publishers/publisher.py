import json
import os
from dataclasses import dataclass
from typing import Any

import neptune
from neptune import Run

from src.storage import object_loader
from src.utils import consts


@dataclass
class Publisher:
    working_dir: str
    config: dict[str, Any]

    def publish(self) -> None:
        run = self.__init_run()
        results_dir_path = os.path.join(self.working_dir, consts.RESULTS_DIR_NAME)
        for result_file_name in os.listdir(results_dir_path):
            result_file_path = os.path.join(results_dir_path, result_file_name)
            result = object_loader.load(result_file_path)
            self.__publish_result(run, result)
        run.stop()

    def __init_run(self) -> Run:
        neptune_details_file_name = self.config["neptune_details_file_name"]
        neptune_details_file_path = os.path.join(consts.LICENSES_DIR_PATH, neptune_details_file_name)
        neptune_details = json.load(open(neptune_details_file_path))
        return neptune.init_run(**neptune_details)

    @staticmethod
    def __publish_result(run: Run, result: dict[str, list[dict[str, float]]]) -> None:
        for name, values in result.items():
            for value in values:
                run[name].append(value)
