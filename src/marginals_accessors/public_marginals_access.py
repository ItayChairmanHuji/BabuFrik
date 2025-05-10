from narwhals import DataFrame

from src.marginals.marginals import Marginals


def get_marginals(private_data: DataFrame) -> Marginals:
    return Marginals(private_data)
