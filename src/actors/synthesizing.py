from typing import Any

from pandas import DataFrame
from snsynth import Synthesizer
from snsynth.mst import MSTSynthesizer

from src.constraints.functional_dependencies import FunctionalDependencies
from src.external_code.kamino_runner import run_kamino
from src.utils import smart_noise_fixes


def generate_synthetic_data(data: DataFrame, fds: FunctionalDependencies, eps: float, model_name: str,
                            model_extra_data: dict[str, Any], sample_size: int) -> DataFrame:
    if model_name == "kamino":
        return run_kamino(data, fds)
    return smart_noise_synthesis(data, eps, model_name, model_extra_data, sample_size)


def smart_noise_synthesis(data: DataFrame, eps: float, model_name: str,
                          model_extra_data: dict[str, Any], sample_size: int) -> DataFrame:
    MSTSynthesizer.compress_domain = smart_noise_fixes.mst_compress_domain
    model = Synthesizer.create(synth=model_name, epsilon=eps, verbose=True, **model_extra_data)
    model.fit(data, categorical_columns=data.columns)
    return model.sample(sample_size)
