from dataclasses import dataclass


@dataclass
class FunctionalDependency:
    source: str
    target: str

    def __str__(self) -> str:
        return f'{self.source} -> {self.target}'

    def __repr__(self) -> str:
        return self.__str__()
