import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATASETS_DIR = os.path.join(ROOT_DIR, 'datasets')
TASKS_CONFIGURATION_DIR = os.path.join(ROOT_DIR, 'tasks_configuration')
LICENSES_DIR = os.path.join(ROOT_DIR, 'licenses')
GUROBI_LICENSE_PATH = os.path.join(LICENSES_DIR, 'gurobi')
WANDB_LICENSE_PATH = os.path.join(LICENSES_DIR, 'weights_and_biases')
ERROR_MARGINS_FILE_NAME = "error_margins.csv"
