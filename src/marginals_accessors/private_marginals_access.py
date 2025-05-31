import pandas as pd
from mbi import Domain, Dataset
from pandas import DataFrame

from src.marginals.marginals import Marginals


def get_marginals(private_data: DataFrame) -> Marginals:
    return Marginals(private_data)

def private_pgm_marginals(data: DataFrame) -> Marginals:
    backup = {}
    for attr in data.columns:
        if pd.to_numeric(data[attr], errors="coerce").isna().all():
            data[attr], backup[attr] = data[attr].factorize()
    domain = Domain(data.columns, data.nunique())
    dataset = Dataset(data, domain)
    # Laplace? What to do here?

