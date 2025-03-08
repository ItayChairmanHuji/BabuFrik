import os
from functools import partial
from typing import Any

import numpy as np
from pandas import DataFrame
from scipy import sparse
from snsynth import Synthesizer as SNSynth
from snsynth.mst import MSTSynthesizer as SNMSTSynthesizer

from src.marginals.marginals import Marginals
from src.utils import consts
from src.utils.node import Node
from src.utils.object_loader import ObjectLoader


def compress_domain(self, data, measurements):
    supports = {}
    new_measurements = []
    for Q, y, sigma, proj in measurements:
        col = proj[0]
        sup = y >= 3 * sigma
        supports[col] = sup
        if supports[col].sum() == y.size:
            new_measurements.append((Q, y, sigma, proj))
        else:
            y2 = np.append(y[sup], y[~sup].sum())
            I2 = np.ones(y2.size)
            I2[-1] = 1.0 / np.sqrt(y.size - y2.size + 1.0)
            y2[-1] /= np.sqrt(y.size - y2.size + 1.0)
            I2 = sparse.diags(I2)
            new_measurements.append((I2, y2, sigma, proj))

    undo_compress_fn = partial(self.reverse_data, supports=supports)

    return self.transform_data(data, supports), new_measurements, undo_compress_fn


class MSTSynthesizer(Node):
    def __init__(self, config: dict[str, Any]):
        super().__init__(config=config,
                         fields=["epsilon", "size_to_sample"])
        SNMSTSynthesizer.compress_domain = compress_domain

    @staticmethod
    def output_file_name() -> str:
        return consts.SYNTHETIC_DATA_FILE_NAME

    def node_action(self, data: DataFrame) -> DataFrame:
        Marginals(data).save(self.working_dir)
        model = self.__train_model(data)
        return self.__sample(model)

    def __train_model(self, data: DataFrame) -> SNSynth:
        model = SNSynth.create(synth="mst", epsilon=self.config["epsilon"], verbose=True)
        model.fit(data, categorical_columns=data.columns.values.tolist(), preprocessor_eps=0)
        model_file_path = os.path.join(self.working_dir, consts.MODEL_FILE_NAME)
        ObjectLoader.save(model, str(model_file_path))
        return model

    def __sample(self, model: SNSynth) -> DataFrame:
        size = self.config["size_to_sample"]
        return model.sample(size)
