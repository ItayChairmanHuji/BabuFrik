import os
import pickle
from typing import Any

from pandas import DataFrame
from snsynth import Synthesizer as ModelCreator
from snsynth.pytorch.nn import PATECTGAN

from src.utils import consts
from src.utils.marginal_calculator import MarginalsCalculator
from src.utils.node import Node


def set_device(self, device):
    self._device = device
    if self._generator is not None:
        self._generator.to(self._device)


class PATECTGANSynthesizer(Node):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config=config,
                         fields=["epsilon", "size_to_sample"])
        PATECTGAN.set_device = set_device

    @staticmethod
    def output_file_path() -> str:
        return consts.SYNTHETIC_DATA_FILE_NAME

    def node_action(self, data: DataFrame) -> DataFrame:
        marginals = MarginalsCalculator.calculate_marginal(data)
        marginals_file_path = os.path.join(self.working_dir, consts.MARGINALS_FILE_NAME)
        with open(marginals_file_path, "wb") as f:
            pickle.dump(marginals, f, protocol=pickle.HIGHEST_PROTOCOL)

        model = ModelCreator.create(synth="patectgan", epsilon=self.config["epsilon"], verbose=True)
        model.fit(data, categorical_columns=data.columns.values.tolist(), preprocessor_eps=0)
        model_file_path = os.path.join(self.working_dir, consts.MODEL_FILE_NAME)
        model.save(model_file_path)

        size = self.config["size_to_sample"]
        return model.sample(size)
