import gurobipy as gp

from src.utils.constraint_adders.constraint_adder import ConstraintAdder
from src.utils.ilp_model import ILPModel
from src.utils.violations_checker import ViolationsChecker


class ViolationsConstraintAdder(ConstraintAdder):
    @staticmethod
    def add_constraint(ilp: ILPModel) -> ILPModel:
        violating_pairs = ViolationsChecker.find_violating_pairs(ilp.data, ilp.fds)
        ilp.model.addConstrs((gp.quicksum(ilp.objective[i] for i in pair) <= 1
                              for pair in violating_pairs), name="violations constraints")
        return ilp
