from dataclasses import dataclass

from src.entities.action import Action
from src.entities.algorithms import QualityFunctions


@dataclass
class Statistics:
    runtime: float
    action: Action
    quality_func: QualityFunctions
    quality: float
