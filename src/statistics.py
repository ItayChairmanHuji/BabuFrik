from dataclasses import dataclass


@dataclass
class Statistics:
    runtime: float
    violations_count: int
    marginals_difference: float
