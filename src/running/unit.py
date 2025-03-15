from dataclasses import dataclass
from typing import Any

from src.analyzers.analyzer import Analyzer
from src.running.service import Service
from src.utils.report import Report


@dataclass
class Unit:
    service: Service
    analyzers: list[Analyzer]

    def run(self, input_file_path: str, dynamic_fields: dict[str, Any]) -> Report:
        report = self.service.run(input_file_path)
        for analyzer in self.analyzers:
            analyzer.analyze(dynamic_fields, report)
        return report
