from dataclasses import dataclass
from typing import Any

from src.running import service_utils
from src.running.service import Service
from src.running.unit import Unit


@dataclass
class Job:
    units: list[Unit]
    fds_file_path: str
    marginals_errors_margins_file_path: str

    def run(self, dynamic_fields: dict[str, Any]) -> None:
        input_file = None
        for unit in self.units:
            report = unit.run(input_file, dynamic_fields)
            input_file = report.output_file_path

    def __create_services(self, working_dir: str, dynamic_fields: dict[str, Any]) -> list[Service]:
        return [self.__create_service(service, working_dir, dynamic_fields) for service in self.services]

    def __create_service(self, service_config_name: str, working_dir: str, dynamic_fields: dict[str, Any]) -> Service:
        service_class, service_config = service_utils.load_service(service_config_name, dynamic_fields)
        return service_class(
            working_dir=working_dir,
            fds_file_path=self.fds_file_path,
            marginals_errors_margins_file_path=self.marginals_errors_margins_file_path,
            config=service_config
        )
