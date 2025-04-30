from dataclasses import dataclass
from typing import Any, Optional

from src.analyzers.analyzer import Analyzer
from src.running.service import Service
from src.utils.message import Message
from src.utils.scenario import Scenario


@dataclass
class Job:
    service: Service
    analyzers: list[Analyzer]
    dynamic_fields: dict[str, list[Any]]
    repetitions: int

    def run(self, message: Message) -> list[Message]:
        print(f"Running {self.service.config.type} {self.service.name}")
        scenarios = [scenario for scenario in self.__get_scenarios(message)]
        return [self.__run_scenario(scenario) for scenario in scenarios]

    def __get_scenarios(self, message: Message) -> list[Scenario]:
        return [Scenario(message=message, dynamic_fields=self.__get_dynamic_fields(dynamic_values))
                for dynamic_values in zip(*self.dynamic_fields.values())]

    def __get_dynamic_fields(self, dynamic_values: tuple[Any]) -> dict[str, Any]:
        return {dynamic_key: dynamic_values
                for dynamic_key, dynamic_values in zip(self.dynamic_fields.keys(), dynamic_values)}

    def __run_scenario(self, scenario: Scenario) -> Message:
        print(
            f"Running {self.service.config.type} {self.service.name} with settings {scenario.dynamic_fields | scenario.message.extra_data}")
        self.__update_service_config(scenario)
        return self.__run_and_analyze(scenario)

    def __update_service_config(self, scenario: Scenario) -> None:
        for dynamic_key, dynamic_value in scenario.dynamic_fields.items():
            self.service.config[dynamic_key] = dynamic_value
            scenario.message.extra_data[dynamic_key] = dynamic_value

    def __run_and_analyze(self, scenario: Scenario) -> Message:
        try:
            message = self.service.run(scenario.message)
            for analyzer in self.analyzers:
                analyzer.analyze(scenario.dynamic_fields, message)
            return message
        except Exception as e:
            print(f"Exception while running {self.service.config.type} {self.service.name}: {e}")