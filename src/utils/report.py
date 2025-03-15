from dataclasses import dataclass

from src.running.service import Service


@dataclass
class Report:
    service: Service
    output_file_path: str
    start_time: float
    end_time: float
