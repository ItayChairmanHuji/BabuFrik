import os

# Base folders
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONFIGURATION_DIR_PATH = os.path.join(ROOT_DIR, "configuration")
RESOURCES_DIR_PATH = os.path.join(ROOT_DIR, "resources")
TASKS_DIR_PATH = os.path.join(ROOT_DIR, "tasks")

# Services configuration folders
GENERATORS_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, "generators")
CLEANERS_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, "cleaner")
SYNTHESIZERS_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, "synthesizers")
REPAIRERS_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, "repairers")
ANALYZERS_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, "analyzers")

# Other configuration dirs
TASKS_CONFIGURATION_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, "tasks")
FUNCTIONAL_DEPENDENCIES_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, "functional_dependencies")
MARGINALS_ERRORS_MARGINS_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, "marginals_errors_margins")
LICENSES_DIR_PATH = os.path.join(CONFIGURATION_DIR_PATH, "licenses")

# Data files names
GENERATED_DATA_FILE_NAME = "original_data.csv"
CLEANED_DATA_FILE_NAME = "cleaned_data.csv"
SYNTHETIC_DATA_FILE_NAME = "synthetic_data.csv"
REPAIRED_DATA_FILE_NAME = "repaired_data.csv"

# Other files names
MARGINALS_FILE_NAME = "marginals.pkl"
MODEL_FILE_NAME = "synthesizer_model"
DEFAULT_TASK_CONFIGURATION_FILE_NAME = "main.json"
