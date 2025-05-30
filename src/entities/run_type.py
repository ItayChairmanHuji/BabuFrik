from enum import EnumType


class RunType(EnumType):
    PRIVATE_DATA = 0
    SYNTHETIC_DATA = 1
    CONSTRAINTS_NUM = 2
    MARGINALS_NUM = 3
    PRIVACY_BUDGET = 4
