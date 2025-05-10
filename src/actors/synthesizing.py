from typing import Any

from pandas import DataFrame
from snsynth import Synthesizer


def generate_synthetic_data(data: DataFrame, training_epsilon: float, model_name: str,
                            unique_values_threshold: int, model_extra_data: dict[str, Any],
                            sample_size: int) -> DataFrame:
    preprocess_epsilon = 0.1
    eps = training_epsilon + preprocess_epsilon
    model = Synthesizer.create(synth=model_name, epsilon=eps, verbose=True)
    continuous_columns = data.columns[data.nunique() > unique_values_threshold].tolist()
    categorical_columns = data.columns.drop(continuous_columns).tolist()
    return model.fit(data, categorical_columns=categorical_columns, continuous_columns=continuous_columns,
                     preprocess_epsilon=preprocess_epsilon, **model_extra_data).sample(sample_size)
