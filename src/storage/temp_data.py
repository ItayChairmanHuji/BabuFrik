import pandas as pd
from pandas import DataFrame


class TempData:
    def __init__(self, input_file_path: str, output_file_path: str):
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.data = DataFrame()

    def __enter__(self):
        if self.input_file_path is not None:
            self.data = pd.read_csv(self.input_file_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.data.to_csv(self.output_file_path, index=False)
