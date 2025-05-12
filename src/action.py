from enum import EnumType


class Action(EnumType):
    CLEANING = "cleaning"
    MARGINALS = "marginals"
    SYNTHESIZING = "synthesizing"
    REPAIRING = "repairing"