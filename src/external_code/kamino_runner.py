import os
import shutil
import uuid
from typing import Any

import pandas as pd
from pandas import DataFrame

from src.utils.kamino_fixes import imports_fix

imports_fix()
from kamino.synthesizer import kamino
from kamino.synthesizer.util import _analyze_privacy
from src.constraints.functional_dependencies import FunctionalDependencies


def kamino_fixes() -> None:


def run_kamino(data: DataFrame, fds: FunctionalDependencies):
    temp_dir_name = str(uuid.uuid4())
    os.mkdir(temp_dir_name)
    try:
        data_path = send_data_to_kamino(data, temp_dir_name)
        constraints_path = send_constraints_to_kamino(fds, temp_dir_name)
        weights_path = send_weights_to_kamino(fds, temp_dir_name) # Might be removed, not sure it's used
        params = get_run_params(data)
        analyze_run_privacy(params)
        return generate_synthetic_data(data_path, constraints_path, params)
    finally:
        shutil.rmtree(temp_dir_name)


def send_data_to_kamino(data: DataFrame, working_dir: str) -> str:
    data_path = os.path.join(working_dir, "data.csv")
    data.to_csv(data_path, index=False)
    return data_path


def send_constraints_to_kamino(fds: FunctionalDependencies, working_dir: str) -> str:
    constraints_path = os.path.join(working_dir, "data.ic")
    fds_dc_format = fds.dc_format
    with open(constraints_path, "w") as constraints_file:
        constraints_file.write(fds_dc_format)
    return constraints_path


def send_weights_to_kamino(fds: FunctionalDependencies, working_dir: str) -> str:
    weights_path = os.path.join(working_dir, "data.w")
    with open(weights_path, "w") as weights_file:
        weights_file.write("++")
        for i in range(fds.dc_format.count('\n')):
            weights_file.write("1000")
    return weights_path


def get_run_params(data: DataFrame) -> dict[str, Any]:
    return {
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


def analyze_run_privacy(params: dict[str, Any]) -> None:
    epsilon2 = _analyze_privacy(params)
    params['epsilon2'] = epsilon2

def generate_synthetic_data(data_path: str, constraints_path: str, params: dict[str,Any]) -> DataFrame:
    kamino.syn_data(
        path_data=data_path,
        path_ic=constraints_path,
        paras=params
    )
    return pd.read_csv(params["path_syn"])