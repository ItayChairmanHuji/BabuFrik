from torch import EnumType


class SynthesizingAlgorithms(EnumType):
    PATECTGAN = "patectgan"
    MST = "mst"
    KAMINO = "kamino"

class MarginalsGenerationAlgorithms(EnumType):
    PUBLIC_MARGINALS = "public_marginals"
    PRIVATE_MARGINALS = "private_marginals"

class RepairAlgorithms(EnumType):
    NO_REPAIR = "no_repair"
    ILP = "ilp"
    LP_VERTEX_COVER = "lp_vertex_cover"
    GREEDY_VERTEX_COVER = "greedy_vertex_cover"
    NETWORKX_VERTEX_COVER = "networkx_vertex_cover"
    GREEDY = "greedy"

class CostFunctions(EnumType):
    LEAVE_ONE_OUT = "leave_one_out"
    SHAPLEY = "shapley"

class QualityFunctions(EnumType):
    VIOLATIONS_COUNT = "violations_count"
    MARGINALS_DISTANCE = "marginals_distance"
    DELETION_OVERHEAD = "deletion_overhead"
    ML_ACCURACY = "ml_accuracy"

class NodesTypes(EnumType):
    CLEANING = "cleaning"
    SYNTHESIZING = "synthesizing"
    MARGINALS = "marginals"
    REPAIRING = "repairing"