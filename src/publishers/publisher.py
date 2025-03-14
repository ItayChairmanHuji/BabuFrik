import os
from dataclasses import dataclass
from typing import Any

import wandb
from pandas import DataFrame
from wandb.apis.public import Run

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
        run.finish()

    def __init_run(self) -> Run:
        api_key_file_name = self.config["api_key_file_name"]
        api_key_file_path = os.path.join(consts.LICENSES_DIR_PATH, api_key_file_name)
        api_key = open(api_key_file_path).read()
        wandb.login(key=api_key)
        return wandb.init(project=self.config["project_name"], name="test-1")

    @staticmethod
    def __publish_result(run: Run, result: dict[str, dict[str, list[float]]]) -> None:
        for name, data in result.items():
            table = wandb.Table(dataframe=DataFrame(data))
            chart_section = name.split("_")[-1]
            chart_name = f"{chart_section}/{name.removesuffix(f"_{chart_section}")}"
            run.log({chart_name: wandb.plot.plot_table(
                data_table=table,
                vega_spec_name="itay-chairman-hebrew-university-of-jerusalem/line",
                fields={"x": table.columns[0], "y": table.columns[1], "stroke": None},
                string_fields={"title": chart_name},
                split_table=True,
            )})
