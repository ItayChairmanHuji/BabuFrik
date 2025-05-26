import numpy as np
from pandas import DataFrame


def clean_data(data: DataFrame, empty_values_threshold: float,
               columns_threshold: int, columns_to_keep: list[str], rows_threshold: int) -> DataFrame:
    result = clean_empty_values(data, empty_values_threshold)
    result = limit_number_of_columns(result, columns_threshold, columns_to_keep)
    return limit_number_of_rows(result, rows_threshold)


def clean_empty_values(data: DataFrame, empty_values_threshold: float) -> DataFrame:
    columns_with_empty_values = data.columns[data.isna().any()]
    columns_to_remove = [column for column in columns_with_empty_values if
                         get_empty_values_rate_in_column(data, column) >= empty_values_threshold]
    columns_to_remove_data = [column for column in columns_with_empty_values if column not in columns_to_remove]
    return data.drop(columns=columns_to_remove).dropna(subset=columns_to_remove_data)


def limit_number_of_columns(data: DataFrame, columns_threshold: int, columns_to_keep: list[str]) -> DataFrame:
    num_of_columns_to_remove = max(len(data.columns) - len(columns_to_keep) - columns_threshold, 0)
    columns_to_remove = (np.random.choice(data.columns.difference(columns_to_keep),
                                          num_of_columns_to_remove, replace=False))
    return data.drop(columns=columns_to_remove)


def limit_number_of_rows(data: DataFrame, rows_threshold: int) -> DataFrame:
    num_of_rows_to_remove = max(len(data) - rows_threshold, 0)
    rows_to_remove = np.random.choice(data.index, num_of_rows_to_remove, replace=False)
    return data.drop(index=rows_to_remove)


def get_empty_values_rate_in_column(data: DataFrame, column: str) -> float:
    num_of_empty_values = data[column].isna().sum()
    total_num_of_values = len(data[column])
    return num_of_empty_values / total_num_of_values
