import os
from pydantic import BaseSettings, Field
import yaml
from api.utils.config import *
from typing import List

# Pydantic class
class Config(BaseSettings):
   BASE_DIR: str = os.getcwd()
   DATA_PATH: str = "data/" 
   OUTPUT_PATH: str = "data/output/"
   CHUNKSIZE: int = 100_000
   TDIGEST_MODEL: str = None
   SAVE_TDIGEST_MODEL: bool = True
   IPCA_MODEL: str = None
   PCA_N_COMPONENTS: int = 3 
   STEPS_LIST: List[int] = [50, 50, 50]
   N_JOBS: int = os.cpu_count()
   RANDOM_STATE: int = 42
   TMAP_NAME: str = "tmap"
   PERMUTATIONS: int = 512
   TMAP_K: int = 20 
   TMAP_NODE_SIZE: int = 5
   TMAP_POINT_SCALE: float = 1.0
   LOG_FILE_PATH: str = "logs/app.log"
   LOGGING_LEVEL: str = "INFO"
   LOGGING_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
   
   
   class Config:
    env_prefix = 'CHELOMBUS_' # ENV variables will have this prefix
    case_sensitive = False

def load_config(user_config_path=None) -> Config:
    """
    Load and merge configurations from defaults, a user-provided file, and evironment variables
    """
    print(user_config_path)
    if user_config_path and os.path.exists(user_config_path):
        config = Config(_env_file=user_config_path)
    else:
        config = Config()
    return config