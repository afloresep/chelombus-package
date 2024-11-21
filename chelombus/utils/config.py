import os

# =======================
# General Configuration
# =======================

# Base directory of the project (default: directory where the script is run)
BASE_DIR = os.getcwd() # Use cwd as base 

# =======================
# File Paths
# =======================
DATA_PATH= os.path.join(BASE_DIR, 'data', 'default_data.cxsmiles')  # Input file path
OUTPUT_PATH= os.path.join(BASE_DIR, 'data', 'default_output/')  # Default output directory

# Cluster data paths
CLUSTER_DATA_PATH = os.path.join(OUTPUT_PATH, 'clustered_output/')
INPUT_TMAP_PATH = os.path.join(OUTPUT_PATH, 'cluster_representatives.csv')
OUTPUT_TMAP_PATH = os.path.join(OUTPUT_PATH, 'maps/')

# Logging paths
LOG_FILE_PATH = os.path.join(BASE_DIR, 'logs', 'app.log')

# =======================
# Data Loading
# =======================
CHUNKSIZE = 10_000_000  # Number of rows per chunk

# =======================
# PCA Parameters
# =======================
PCA_N_COMPONENTS = 3  # Number of PCA dimensions
BINS = [50, 50, 50]  # Number of bins on each pca dimension 

# =======================
# Parallel Processing
# =======================
N_JOBS = os.cpu_count()  # Default: use all available CPU cores

# =======================
# Randomization
# =======================
RANDOM_STATE = 42  # Reproducibility seed

# =======================
# TMAP Configuration
# =======================
TMAP_NAME = 'representative_cluster'
PERMUTATIONS = 512  # MinHash permutations
TMAP_K = 20  # Number of neighbors
TMAP_NODE_SIZE = 1 / 40
TMAP_POINT_SCALE = 3.0

# =======================
# Logging Configuration
# =======================
LOGGING_LEVEL = 'INFO'  # Logging level
LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
