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
        neptune_details = self.config["neptune_details"]
        run = neptune.init_run(**neptune_details)
        results_dir_path = os.path.join(self.working_dir, consts.RESULTS_DIR_NAME)
        for result_file_name in os.listdir(results_dir_path):
            result_file_path = os.path.join(results_dir_path, result_file_name)
            result = object_loader.load(result_file_path)
            self.__publish_result(run, result)
        run.stop()

    @staticmethod
    def __publish_result(run: Run, result: dict[str, list[dict[str, float]]]) -> None:
        for name, values in result.items():
            for value in values:
                run[name].append(value)
