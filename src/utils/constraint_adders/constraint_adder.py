from abc import ABC, abstractmethod

from src.utils.ilp_model import ILPModel


class ConstraintAdder(ABC):

    @staticmethod
    @abstractmethod
    def add_constraint(ilp: ILPModel) -> ILPModel:
        raise NotImplementedError("add_constraint is not implemented")