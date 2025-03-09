from dataclasses import dataclass


@dataclass
class Report:
    service_name: str
    output_file_path: str
    start_time: float
    end_time: float
