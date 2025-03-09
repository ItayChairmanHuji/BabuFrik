import pandas as pd

from src.marginals.multi_index_series import MultiIndexSeries


class MarginalsErrorsMargins:
    def __init__(self, file_path: str):
        self.errors_margins = MultiIndexSeries(
            data=pd.read_csv(str(file_path), index_col=[0, 1], skipinitialspace=True).squeeze(),
            default_value=1
        )

    def __getitem__(self, key: tuple[str, ...]) -> float:
        return self.errors_margins[key]
