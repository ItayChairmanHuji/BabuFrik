import os
import uuid
from typing import Any

import pandas as pd
from pandas import DataFrame
from snsynth import Synthesizer
from snsynth.mst import MSTSynthesizer

import kamino.synthesizer.kamino
from src import smart_noise_fixes
from src.constraints.functional_dependencies import FunctionalDependencies


def generate_synthetic_data(data: DataFrame, fds: FunctionalDependencies, training_epsilon: float, model_name: str,
                            unique_values_threshold: float, model_extra_data: dict[str, Any],
                            sample_size: int) -> DataFrame:
    if model_name == "kamino":
        return run_kamino(data, fds)
    MSTSynthesizer.compress_domain = smart_noise_fixes.mst_compress_domain
    continuous_columns = data.columns[data.nunique() > unique_values_threshold].tolist()
    categorical_columns = data.columns.drop(continuous_columns).tolist()
    preprocess_epsilon = 1
    eps = training_epsilon + preprocess_epsilon if len(continuous_columns) != 0 else training_epsilon
    model = Synthesizer.create(synth=model_name, epsilon=eps, verbose=True, **model_extra_data)
    model.fit(data, categorical_columns=categorical_columns, continuous_columns=continuous_columns,
              preprocessor_eps=preprocess_epsilon)
    return model.sample(sample_size)


def run_kamino(data: DataFrame, fds: FunctionalDependencies):
    temp_dir_name = str(uuid.uuid4())
    os.mkdir(temp_dir_name)
    data_path = os.path.join(temp_dir_name, "data.csv")
    data.to_csv(data_path, index=False)
    constraints_path = os.path.join(temp_dir_name, "data.ic")
    fds_dc_format = fds.dc_format
    with open(constraints_path, "w") as constraints_file:
        constraints_file.write(fds_dc_format)
    weights_path = os.path.join(temp_dir_name, "data.w")
    with open(weights_path, "w") as weights_file:
        weights_file.write("++")
        for i in range(fds_dc_format.count('\n')):
            weights_file.write("1000")
    params = {
        'reuse_embedding': True,
        'dp': True,
        'n_row': len(data),
        'n_col': len(data.columns),
        'epsilon1': .1,
        'l2_norm_clip': 1.0,
        'noise_multiplier': 1.1,
        'minibatch_size': 23,
        'microbatch_size': 1,
        'delta': 1e-6,
        'learning_rate': 1e-4,
        'iterations': 1600,
        'AR': False,
        'MCMC': 0,
    }
    kamino.synthesizer.kamino.syn_data(
        path_data=data_path,
        path_ic=weights_path,
        paras=params
    )
    return pd.read_csv(params["path_syn"])
