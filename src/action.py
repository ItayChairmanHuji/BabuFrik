from enum import EnumType


class Action(EnumType):
    INITIALISING = "initialising"
    CLEANING = "cleaning"
    MARGINALS = "marginals"
    SYNTHESIZING = "synthesizing"
    REPAIRING = "repairing"
    TERMINATING = "terminating"
