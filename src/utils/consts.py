import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONFIGURATION_DIR_PATH = os.path.join(ROOT_DIR, 'configuration')
RESOURCES_DIR_PATH = os.path.join(ROOT_DIR, 'resources')
ROUTES_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, 'routes')
NODES_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, 'nodes')
TASKS_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, 'tasks')
FUNCTIONAL_DEPENDENCIES_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, 'functional_dependencies')
LICENSES_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, 'licenses')
MARGINALS_ERROR_FACTORS_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, 'marginals_errors_factors')
FUNCTIONAL_DEPENDENCIES_FILE_NAME = "fd.json"
MARGINALS_FILE_NAME = "marginals.pkl"
MODEL_FILE_NAME = "model.pt"
GENERATED_DATA_FILE_NAME = "original_data.csv"
CLEANED_DATA_FILE_NAME = "cleaned_data.csv"
SYNTHETIC_DATA_FILE_NAME = "synthetic_data.csv"
REPAIRED_DATA_FILE_NAME = "repaired_data.csv"
MARGINALS_ERRORS_FACTORS_FILE_NAME = "marginals_errors_factors.csv"
MAIN_CONFIGURATION_FILE_PATH = os.path.join(CONFIGURATION_DIR_PATH, 'main.json')
