import os
from typing import Any

from pandas import DataFrame
from snsynth import Synthesizer as SNSynth

from src.utils import consts
from src.utils.marginal_calculator import Marginals
from src.utils.node import Node


class MSTSynthesizer(Node):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config=config,
                         fields=["epsilon", "size_to_sample"])

    @staticmethod
    def output_file_path() -> str:
        return consts.SYNTHETIC_DATA_FILE_NAME

    def node_action(self, data: DataFrame) -> DataFrame:
        Marginals(data).save(self.working_dir)
        model = self.__train_model(data)
        return self.__sample(model)

    def __train_model(self, data: DataFrame) -> SNSynth:
        model = SNSynth.create(synth="mst", epsilon=self.config["epsilon"], verbose=True)
        model.fit(data, preprocessor_eps=0)
        model_file_path = os.path.join(self.working_dir, consts.MODEL_FILE_NAME)
        model.save(model_file_path)
        return model

    def __sample(self, model: SNSynth) -> DataFrame:
        size = self.config["size_to_sample"]
        return model.sample(size)
