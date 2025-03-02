from src.utils.constraint_adders.constraint_adder import ConstraintAdder
from src.utils.ilp_model import ILPModel


class TrivialSolutionConstraintAdder(ConstraintAdder):
    @staticmethod
    def add_constraint(ilp: ILPModel) -> ILPModel:
        ilp.model.addConstr(ilp.model.objective.sum() >= 1)
        return ilp
