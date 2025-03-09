import os
import time

from pandas import DataFrame
from snsynth import Synthesizer as SNSynth
from snsynth.pytorch.nn import PATECTGAN

from src.marginals.marginals import Marginals
from src.runner.service import Service
from src.utils import consts, smartnoise_fixes


class PATECTGANSynthesizer(Service):
    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["epsilon", "size_to_sample"]

    @staticmethod
    def output_file_name() -> str:
        return consts.SYNTHETIC_DATA_FILE_NAME

    def service_action(self, data: DataFrame) -> DataFrame:
        PATECTGAN.set_device = smartnoise_fixes.patectgan_set_device
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
