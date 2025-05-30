from pandas import DataFrame

from src.constraints.functional_dependencies import FunctionalDependencies
from src.entities import consts
from src.entities.algorithms import RepairAlgorithms
from src.marginals.marginals import Marginals
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.reapairs import greedy_repair, lp_vertex_cover_repair, ilp_repair, greedy_vertex_cover_repair, \
    networkx_vertex_cover_repair


def repair_data(data: DataFrame, fds: FunctionalDependencies,
                marginals: Marginals, marginals_error_margins: MarginalsErrorsMargins,
                repair_algorithm: RepairAlgorithms) -> DataFrame:
    match repair_algorithm:
        case RepairAlgorithms.NO_REPAIR:
            return data
        case RepairAlgorithms.ILP:
            return ilp_repair.repair_data(data, fds, marginals_error_margins, marginals, consts.GUROBI_LICENSE_PATH)
        case RepairAlgorithms.LP_VERTEX_COVER:
            return lp_vertex_cover_repair.repair_data(data, fds, marginals, consts.GUROBI_LICENSE_PATH)
        case RepairAlgorithms.GREEDY_VERTEX_COVER:
            return greedy_vertex_cover_repair.repair_data(data, fds, marginals, marginals_error_margins)
        case RepairAlgorithms.NETWORKX_VERTEX_COVER:
            return networkx_vertex_cover_repair.repair_data(data, fds, marginals, marginals_error_margins)
        case RepairAlgorithms.GREEDY_REPAIR:
            return greedy_repair.repair_data(data, fds, marginals, marginals_error_margins)
        case _:
            raise NotImplementedError(f"Repairing algorithm {repair_algorithm} is not supported.")
