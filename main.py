import json
import os
import sys
import uuid
from typing import Union, TypeVar

import pandas as pd
import pyvacy
import ray
from pandas import DataFrame

from src.constraints import functional_dependencies
from src.pipelines.num_of_private_marginals_pipeline import NumOfPrivateMarginalsPipeline
from src.pipelines.privacy_budget_pipeline import PrivacyBudgetPipeline

sys.modules['pyvacy.pyvacy'] = pyvacy
sys.path.append("./kaminos")

from src.entities import consts
from src.entities.configuration import Configuration
from src.constraints.functional_dependencies import FunctionalDependencies
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.pipelines.constraints_num_pipeline import ConstraintsNumPipeline
from src.pipelines.pipeline import Pipeline
from src.pipelines.private_data_pipeline import PrivateDataPipeline
from src.entities.run_type import RunType
from src.pipelines.synthetic_data_pipeline import SyntheticDataPipeline
from src.pipelines.results_publisher import ResultsPublisher

T = TypeVar("T", bound=Pipeline)


def load_configuration() -> Configuration:
    if len(sys.argv) < 2:
        raise Exception("No configuration file specified")

    config_file_name = sys.argv[1]
    config_file_path = str(os.path.join(consts.TASKS_CONFIGURATION_DIR, config_file_name))
    return Configuration(**json.load(open(config_file_path)))


def load_dataset(config: Configuration) -> DataFrame:
    data_path = os.path.join(consts.DATASETS_DIR, config.dataset_name, f"{config.dataset_name}.csv")
    return pd.read_csv(data_path)


def load_fds(config: Configuration, run_type: RunType) -> Union[FunctionalDependencies | list[FunctionalDependencies]]:
    dataset_directory = os.path.join(consts.DATASETS_DIR, config.dataset_name)
    if run_type != RunType.CONSTRAINTS_NUM:
        return functional_dependencies.load_fds_file(os.path.join(dataset_directory, config.fds))
    return [functional_dependencies.load_fds_file(os.path.join(dataset_directory, fds)) for fds in config.fds]


def load_marginals_error_margins(config: Configuration) -> MarginalsErrorsMargins:
    error_margins_file_path = os.path.join(consts.DATASETS_DIR, config.dataset_name, consts.ERROR_MARGINS_FILE_NAME)
    if not os.path.exists(error_margins_file_path):
        FileNotFoundError("Error margins file not found")
    return MarginalsErrorsMargins(str(error_margins_file_path))


def get_pipeline_type(run_type: RunType) -> type[T]:
    match run_type:
        case RunType.PRIVATE_DATA:
            return PrivateDataPipeline
        case RunType.SYNTHETIC_DATA:
            return SyntheticDataPipeline
        case RunType.CONSTRAINTS_NUM:
            return ConstraintsNumPipeline
        case RunType.MARGINALS_NUM:
            return NumOfPrivateMarginalsPipeline
        case RunType.PRIVACY_BUDGET:
            return PrivacyBudgetPipeline


def main():
    run_id = str(uuid.uuid4())
    config = load_configuration()
    data = load_dataset(config)
    run_type = config.run_type
    fds = load_fds(config, run_type)
    marginals_errors = load_marginals_error_margins(config)
    pipeline_type = get_pipeline_type(run_type)
    pipeline_type(run_id=run_id,
                  data=data,
                  config=config,
                  fds=fds,
                  marginals_errors_margins=marginals_errors,
                  results_publisher=ResultsPublisher(run_id=run_id, config=config)).run()


if __name__ == '__main__':
    ray.init(include_dashboard=False)
    sys.modules['pyvacy.pyvacy'] = pyvacy
    main()
