from dataclasses import dataclass


@dataclass
class TaskConfiguration:
    services: list[str]
    functional_dependencies_file_name: str
    marginals_errors_margins_file_name: str
    results_dashboard_api_key_file_name: str
    results_dashboard_project_name: str
    working_dir: str = None
