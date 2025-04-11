from dataclasses import dataclass
from itertools import chain
from typing import Any

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
        print(f"Running service {self.service.name}")
        return [self.__run_scenario(scenario) for scenario in self.__get_scenarios(message)]

    def __get_scenarios(self, message: Message) -> list[Scenario]:
        return chain.from_iterable(self.__get_repeated_scenario(message, dynamic_values)
                                   for dynamic_values in zip(*self.dynamic_fields.values()))

    def __get_repeated_scenario(self, message: Message, dynamic_values: tuple[Any]) -> list[Scenario]:
        return [Scenario(message=message, dynamic_fields=self.__get_dynamic_fields(dynamic_values))
                for _ in range(self.repetitions)]

    def __get_dynamic_fields(self, dynamic_values: tuple[Any]) -> dict[str, Any]:
        return {dynamic_key: dynamic_values
                for dynamic_key, dynamic_values in zip(self.dynamic_fields.keys(), dynamic_values)}

    def __run_scenario(self, scenario: Scenario) -> Message:
        print(f"Running service {self.service.name} with settings {scenario.dynamic_fields}")
        self.__update_service_config(scenario)
        return self.__run_and_analyze(scenario)

    def __update_service_config(self, scenario: Scenario) -> None:
        for dynamic_key, dynamic_value in scenario.dynamic_fields.items():
            self.service.config[dynamic_key] = dynamic_value
            scenario.message.extra_data[dynamic_key] = dynamic_value

    def __run_and_analyze(self, scenario: Scenario) -> Message:
        message = self.service.run(scenario.message)
        for analyzer in self.analyzers:
            analyzer.analyze(scenario.dynamic_fields, message)
        return message
