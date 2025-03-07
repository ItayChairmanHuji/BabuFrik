import os
import time
from typing import Any

from pandas import DataFrame
from snsynth import Synthesizer as SNSynth
from snsynth.pytorch.nn import PATECTGAN

from src.utils import consts
from src.utils.marginals import Marginals
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
        Marginals(data).save(self.working_dir)
        model = self.__train_model(data)
        return self.__sample(model)

    def __train_model(self, data: DataFrame) -> SNSynth:
        start_time = time.time()
        model = SNSynth.create(synth="patectgan", epsilon=self.config["epsilon"], verbose=True)
        end_time = time.time()
        print(f"Time taken marginals calculation: {end_time - start_time}")
        model.fit(data, categorical_columns=data.columns.values.tolist(), preprocessor_eps=0)
        model_file_path = os.path.join(self.working_dir, consts.MODEL_FILE_NAME)
        model.save(model_file_path)
        return model

    def __sample(self, model: SNSynth) -> DataFrame:
        size = self.config["size_to_sample"]
        return model.sample(size)
