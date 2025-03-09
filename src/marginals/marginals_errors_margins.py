import os

import pandas as pd

from src.utils import consts
from src.marginals.multi_index_series import MultiIndexSeries


class MarginalsErrorsMargins:
    def __init__(self, file_name: str):
        file_path = os.path.join(consts.MARGINALS_ERROR_FACTORS_DIR_PATH, file_name)
        self.errors_margins = MultiIndexSeries(
            data=pd.read_csv(str(file_path), index_col=[0, 1], skipinitialspace=True).squeeze(),
            default_value=1
        )

    def __getitem__(self, key: tuple[str, ...]) -> float:
        return self.errors_margins[key]
