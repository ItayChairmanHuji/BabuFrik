import os
from typing import Any

from pandas import DataFrame
from snsynth import Synthesizer as SNSynth
from snsynth.mst import MSTSynthesizer as SNMSTSynthesizer

from src.marginals.marginals import Marginals
from src.running.service import Service
from src.storage import object_loader
from src.synthesizers import smartnoise_fixes
from src.utils import consts


class MSTSynthesizer(Service):
    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["epsilon", "size_to_sample"]

    @staticmethod
    def output_file_name() -> str:
        return consts.SYNTHETIC_DATA_FILE_NAME

    def service_action(self, data: DataFrame) -> DataFrame:
        SNMSTSynthesizer.compress_domain = smartnoise_fixes.mst_compress_domain
        self.__save_marginals(data)
        model = self.__train_model(data)
        return self.__sample(model)

    def __save_marginals(self, data: DataFrame) -> None:
        marginals_path = os.path.join(self.working_dir.path, consts.MARGINALS_FILE_NAME)
        object_loader.save(Marginals(data), marginals_path)
        self.extra_data["marginals_file_path"] = marginals_path

    def __train_model(self, data: DataFrame) -> SNSynth:
        model = SNSynth.create(synth="mst", epsilon=self.config["epsilon"], verbose=True)
        model.fit(data, categorical_columns=data.columns.values.tolist(), preprocessor_eps=0)
        self.__save_model(model)
        return model

    def __save_model(self, model: Any) -> None:
        model_file_path = os.path.join(self.working_dir.path, consts.MODEL_FILE_NAME)
        object_loader.save(model, str(model_file_path))
        self.extra_data["model_file_path"] = model_file_path

    def __sample(self, model: SNSynth) -> DataFrame:
        size = self.config["size_to_sample"]
        return model.sample(size)
