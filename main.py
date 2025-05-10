import json
import os
import sys
import uuid
from typing import Union

import pandas as pd
from pandas import DataFrame

import wandb
from src import utils, consts
from src.configuration import Configuration
from src.constraints.functional_dependencies import FunctionalDependencies
from src.marginals.marginals_errors_margins import MarginalsErrorsMargins
from src.pipelines.constraints_num_pipeline import ConstraintsNumPipeline
from src.pipelines.pipeline import Pipeline
from src.pipelines.private_data_pipeline import PrivateDataPipeline
from src.pipelines.run_type import RunType
from src.pipelines.synthetic_data_pipeline import SyntheticDataPipeline
from src.results_publisher import ResultsPublisher


def load_configuration() -> Configuration:
    if len(sys.argv) < 2:
        raise Exception("No configuration file specified")

    config_file_name = sys.argv[1]
    config_file_path = str(os.path.join(consts.TASKS_CONFIGURATION_DIR, config_file_name))
    return Configuration(**json.load(open(config_file_path)))


def load_dataset(config: Configuration) -> DataFrame:
    data_path = os.path.join(consts.DATASETS_DIR, config.dataset_name, f"{config.dataset_name}.csv")
    return pd.read_csv(data_path)


def get_run_type(config: Configuration) -> RunType:
    if utils.input_type_validation(config.private_data_size, config.synthetic_data_size, config.fds):
        return RunType.PRIVATE_DATA
    elif utils.input_type_validation(config.synthetic_data_size, config.fds, config.private_data_size):
        return RunType.SYNTHETIC_DATA
    elif utils.input_type_validation(config.fds, config.private_data_size, config.synthetic_data_size):
        return RunType.CONSTRAINTS_NUM
    else:
        raise Exception("Invalid configuration input")


def load_fds(config: Configuration, run_type: RunType) -> Union[FunctionalDependencies | list[FunctionalDependencies]]:
    dataset_directory = os.path.join(consts.DATASETS_DIR, config.dataset_name)
    if run_type != RunType.CONSTRAINTS_NUM:
        return utils.load_fds_file(os.path.join(dataset_directory, config.fds))
    return [utils.load_fds_file(os.path.join(dataset_directory, fds)) for fds in config.fds]


def load_marginals_error_margins(config: Configuration) -> MarginalsErrorsMargins:
    error_margins_file_path = os.path.join(consts.DATASETS_DIR, config.dataset_name, consts.ERROR_MARGINS_FILE_NAME)
    if not os.path.exists(error_margins_file_path):
        FileNotFoundError("Error margins file not found")
    return MarginalsErrorsMargins(str(error_margins_file_path))


def create_pipeline(run_id: str, data: DataFrame, config: Configuration,
                    fds: Union[FunctionalDependencies | list[FunctionalDependencies]],
                    marginals_errors_margins: MarginalsErrorsMargins, run_type: RunType) -> Pipeline:
    match run_type:
        case RunType.PRIVATE_DATA:
            return PrivateDataPipeline(run_id=run_id,
                                       data=data,
                                       config=config,
                                       fds=fds,
                                       marginals_errors_margins=marginals_errors_margins,
                                       results_publisher=ResultsPublisher(run_id=run_id, config=config))
        case RunType.SYNTHETIC_DATA:
            return SyntheticDataPipeline(run_id=run_id,
                                         data=data,
                                         config=config,
                                         fds=fds,
                                         marginals_errors_margins=marginals_errors_margins,
                                         results_publisher=ResultsPublisher(run_id=run_id, config=config))
        case RunType.CONSTRAINTS_NUM:
            return ConstraintsNumPipeline(run_id=run_id,
                                          data=data,
                                          config=config,
                                          fds=fds,
                                          marginals_errors_margins=marginals_errors_margins,
                                          results_publisher=ResultsPublisher(run_id=run_id, config=config))


def test():
    api_key = open(consts.WANDB_LICENSE_PATH).read().strip()
    # os.environ["WANDB_SILENT"] = "True"
    wandb.login(key=api_key)


def main():
    run_id = str(uuid.uuid4())
    config = load_configuration()
    data = load_dataset(config)
    run_type = get_run_type(config)
    fds = load_fds(config, run_type)
    marginals_errors = load_marginals_error_margins(config)
    create_pipeline(run_id, data, config, fds, marginals_errors, run_type).run()


if __name__ == '__main__':
    main()
