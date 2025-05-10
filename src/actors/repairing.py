from pandas import DataFrame

from src import consts
from src.constraints.functional_dependencies import FunctionalDependencies
from src.marginals.marginals import Marginals
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.reapairs import greedy_repair, vertex_cover_repair, ilp_repair


def repair_data(data: DataFrame, fds: FunctionalDependencies,
                marginals: Marginals, marginals_error_margins: MarginalsErrorsMargins,
                repair_algorithm: str) -> DataFrame:
    match repair_algorithm:
        case "ilp":
            return ilp_repair.repair_data(data, fds, marginals_error_margins, marginals, consts.GUROBI_LICENSE_PATH)
        case "vertex_cover":
            return vertex_cover_repair.repair_data(data, fds, marginals, consts.GUROBI_LICENSE_PATH)
        case "greedy":
            return greedy_repair.repair_data(data, fds, marginals, marginals_error_margins)
        case _:
            raise NotImplementedError(f"Repairing algorithm {repair_algorithm} is not supported.")
