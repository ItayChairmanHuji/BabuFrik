import os
from abc import ABC, abstractmethod
from typing import Any

import pandas as pd
from pandas import DataFrame
from sklearn.preprocessing import LabelEncoder
from snsynth import Synthesizer as SNSynth

from src.marginals.marginals import Marginals
from src.running.service import Service
from src.storage import object_loader
from src.utils import consts


class SmartNoiseSynthesizerBase(Service, ABC):
    @abstractmethod
    def smart_noise_fixes(self) -> None:
        raise NotImplementedError("Fixes function was not implemented")

    @property
    @abstractmethod
    def smart_noise_name(self) -> str:
        raise NotImplementedError("Name was not implemented")

    @staticmethod
    def mandatory_fields() -> list[str]:
        return ["training_eps", "preprocessor_eps", "size_to_sample", "unique_values_threshold"]

    @staticmethod
    def output_file_name() -> str:
        return consts.SYNTHETIC_DATA_FILE_NAME

    def service_action(self, data: DataFrame) -> DataFrame:
        self.smart_noise_fixes()
        self.__save_marginals(data)
        model = self.__train_model(data)
        return self.__sample(model)

    def __save_marginals(self, data: DataFrame) -> None:
        marginals_path = os.path.join(self.working_dir.path, consts.MARGINALS_FILE_NAME)
        object_loader.save(Marginals(data), marginals_path)
        self.extra_data["marginals_file_path"] = marginals_path

    def __train_model(self, data: DataFrame) -> SNSynth:
        eps = self.config["training_eps"] + self.config["preprocessor_eps"]
        model = SNSynth.create(synth=self.smart_noise_name, epsilon=eps, verbose=True)
        continuous_columns = data.columns[data.nunique() > self.config["unique_values_threshold"]].tolist()
        le = LabelEncoder()
        for continuous_column in continuous_columns:
            as_numeric = pd.to_numeric(data[continuous_column], errors='coerce')
            data[continuous_column] = as_numeric if as_numeric.notnull().all() \
                else le.fit_transform(data[continuous_column])
        categorical_columns = data.columns.drop(continuous_columns).tolist()
        model.fit(data, categorical_columns=categorical_columns,
                  continuous_columns=continuous_columns, preprocessor_eps=self.config["preprocessor_eps"])
        self.__save_model(model)
        return model

    def __save_model(self, model: Any) -> None:
        model_file_path = os.path.join(self.working_dir.path, consts.MODEL_FILE_NAME)
        object_loader.save(model, model_file_path)
        self.extra_data["model_file_path"] = model_file_path

    def __sample(self, model: SNSynth) -> DataFrame:
        size = self.config["size_to_sample"]
        return model.sample(size)
