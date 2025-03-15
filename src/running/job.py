import os
import uuid
from dataclasses import dataclass, replace
from typing import Any

from src.analyzers.analyzer import Analyzer
from src.running.service import Service
from src.utils.message import Message
from src.utils.report import Report
from src.utils.scenario import Scenario


@dataclass
class Job:
    service: Service
    analyzers: list[Analyzer]
    dynamic_fields: dict[str, list[Any]]

    def run(self, message: Message) -> list[Report]:
        print(f"Running service {self.service.name}")
        return [self.__run_scenario(scenario) for scenario in self.__get_scenarios(message)]

    def __get_scenarios(self, message: Message) -> list[Scenario]:
        dynamic_values_list = list(zip(*self.dynamic_fields.values()))  # Inserted manually, so assuming small
        scenarios_ids = [str(uuid.uuid4())
                         for _ in range(len(dynamic_values_list))] if len(dynamic_values_list) == 0 else [""]
        return [
            Scenario(
                message=replace(message, working_dir=os.path.join(message.working_dir, scenario_id)),
                dynamic_fields=self.__get_dynamic_fields(dynamic_values)
            )
            for scenario_id, dynamic_values in zip(scenarios_ids, *dynamic_values_list)
        ]

    def __get_dynamic_fields(self, dynamic_values: tuple[Any]) -> dict[str, Any]:
        return {dynamic_key: dynamic_values
                for dynamic_key, dynamic_values in zip(self.dynamic_fields.keys(), dynamic_values)}

    def __run_scenario(self, scenario: Scenario) -> Report:
        print(f"Running service {self.service.name} with settings {scenario.dynamic_fields}")
        self.__update_service_config(scenario.dynamic_fields)
        return self.__run_and_analyze(scenario)

    def __update_service_config(self, dynamic_fields: dict[str, Any]) -> None:
        for dynamic_key, dynamic_value in dynamic_fields.items():
            self.service.config[dynamic_key] = dynamic_value

    def __run_and_analyze(self, scenario: Scenario) -> Report:
        report = self.service.run(scenario.message)
        for analyzer in self.analyzers:
            analyzer.analyze(scenario.dynamic_fields, report)
        return report
