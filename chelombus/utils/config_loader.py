import os
import yaml
from config import *

def load_config(user_config_path=None):
    """
    Load and merge a user-provided configuration file with the default config.
    """
    # Start with the default config
    config = {
        "BASE_DIR": BASE_DIR,
        "DATA_FILE_PATH": DATA_FILE_PATH,
        "OUTPUT_FILE_PATH": OUTPUT_FILE_PATH,
        "CHUNKSIZE": CHUNKSIZE,
        "PCA_N_COMPONENTS": PCA_N_COMPONENTS,
        "STEPS_LIST": STEPS_LIST,
        "N_JOBS": N_JOBS,
        "RANDOM_STATE": RANDOM_STATE,
        "TMAP_NAME": TMAP_NAME,
        "PERMUTATIONS": PERMUTATIONS,
        "TMAP_K": TMAP_K,
        "TMAP_NODE_SIZE": TMAP_NODE_SIZE,
        "TMAP_POINT_SCALE": TMAP_POINT_SCALE,
        "LOG_FILE_PATH": LOG_FILE_PATH,
        "LOGGING_LEVEL": LOGGING_LEVEL,
        "LOGGING_FORMAT": LOGGING_FORMAT,
    }

    # Merge with the user-provided config if it exists
    if user_config_path and os.path.exists(user_config_path):
        with open(user_config_path, "r") as f:
            user_config = yaml.safe_load(f)
            config.update(user_config)

    # Dynamically resolve paths relative to BASE_DIR
    config["DATA_FILE_PATH"] = os.path.abspath(config["DATA_FILE_PATH"])
    config["OUTPUT_FILE_PATH"] = os.path.abspath(config["OUTPUT_FILE_PATH"])
    config["LOG_FILE_PATH"] = os.path.abspath(config["LOG_FILE_PATH"])

    return config
