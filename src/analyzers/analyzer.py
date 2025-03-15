from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Any

import wandb
from wandb import Table
from wandb.apis.public import Run

from src.utils.configuration import Configuration
from src.utils.report import Report


@dataclass
class Analyzer(ABC):
    run: Run
    table: Table
    config: Configuration

    @property
    def name(self):
        return self.config["analyzer_name"]

    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["x_axis", "vega_spec_name"]

    def analyze(self, dynamic_fields: dict[str, Any], report: Report) -> None:
        if not self.should_analyze(report):
            return

        self.table.add_data(dynamic_fields[self.config["x_axis"]], self.analyzer_action(report))
        service_name = report.service.name
        wandb.plot.line
        self.run.log({f"runtime/{service_name}": wandb.plot.plot_table(
            data_table=self.table,
            vega_spec_name=self.config["vega_spec_name"],  # "itay-chairman-hebrew-university-of-jerusalem/line",
            fields={"x": self.table.columns[0], "y": self.table.columns[1], "stroke": None},
            string_fields={"title": service_name},
            split_table=True,
        )})

    @abstractmethod
    def analyzer_action(self, report: Report) -> float:
        raise NotImplementedError('Analyzer must implement this method')

    def should_analyze(self, report: Report) -> bool:
        return self.name in report.service.analyzers
