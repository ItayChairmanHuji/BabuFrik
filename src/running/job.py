from dataclasses import dataclass
from typing import Any

from src.running import service_utils
from src.running.service import Service
from src.utils.report import Report


@dataclass
class Job:
    services: list[str]
    fds_file_path: str
    marginals_errors_margins_file_path: str

    def run(self, working_dir: str, dynamic_fields: dict[str, Any]) -> list[Report]:
        reports = []
        input_file = None
        for service in self.__create_services(working_dir, dynamic_fields):
            report = service.run(input_file)
            input_file = report.output_file_path
            reports.append(report)
        return reports

    def __create_services(self, working_dir: str, dynamic_fields: dict[str, Any]) -> list[Service]:
        return [self.__create_service(service, working_dir, dynamic_fields) for service in self.services]

    def __create_service(self, service_name: str, working_dir: str, dynamic_fields: dict[str, Any]) -> Service:
        service_class = service_utils.load_service_class(service_name)
        mandatory_fields = service_class.mandatory_fields()
        return service_class(
            working_dir=working_dir,
            fds_file_path=self.fds_file_path,
            marginals_errors_margins_file_path=self.marginals_errors_margins_file_path,
            config=service_utils.load_service_configuration(service_name, mandatory_fields, dynamic_fields)
        )
