from typing import Any

from pandas import DataFrame
from snsynth import Synthesizer
from snsynth.mst import MSTSynthesizer

from src import smart_noise_fixes


def generate_synthetic_data(data: DataFrame, training_epsilon: float, model_name: str,
                            unique_values_threshold: int, model_extra_data: dict[str, Any],
                            sample_size: int) -> DataFrame:
    MSTSynthesizer.compress_domain = smart_noise_fixes.mst_compress_domain
    preprocess_epsilon = 0.1
    eps = training_epsilon + preprocess_epsilon
    model = Synthesizer.create(synth=model_name, epsilon=eps, verbose=True, **model_extra_data)
    continuous_columns = data.columns[data.nunique() > unique_values_threshold].tolist()
    categorical_columns = data.columns.drop(continuous_columns).tolist()
    model.fit(data, categorical_columns=categorical_columns, continuous_columns=continuous_columns,
              preprocessor_eps=preprocess_epsilon)
    return model.sample(sample_size)
