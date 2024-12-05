# File to run the Clustering pipeline
import os
import glob 
import re 
import sys
import time
import logging 
import argparse
import shutil
import h5py
import numpy as np
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from sklearn.decomposition import IncrementalPCA
from memory_profiler import profile
import gc
import pandas as pd

# This allows Python to recognize the src/ directory as a package and facilitates absolute imports from it.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import configurations and modules
from config import (INPUT_TMAP_PATH, OUTPUT_TMAP_PATH, CLUSTER_DATA_PATH, BASE_DIR,
                    LOGGING_LEVEL, LOGGING_FORMAT, LOG_FILE_PATH, N_JOBS, TMAP_K, TMAP_NAME)
from src.tmap_generator import TmapGenerator

def parse_arguments():
    parser = argparse.ArgumentParser(description="Process fingerprints with flexible options.")
    
    # Optional arguments to override config settings
    parser.add_argument('--l', type=str, help="Choose whether you want to see the primary TMAP (cluster representatives) or a secondary TMAP (indicated by cluster_id = int_int_int")
    parser.add_argument('--data-file', type=str, default=INPUT_TMAP_PATH, help="Input data file path.")
    parser.add_argument('--log', type=bool, default=False, help="Saving logs to output.log file. Default False")
    parser.add_argument('--log-level', type=str, default=LOGGING_LEVEL,
                                choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help="Logging verbosity level.")
    parser.add_argument('--output-dir', type=str, default=OUTPUT_TMAP_PATH, help='Output directory for TMAP files (i.e. HTML and .js)')
    parser.add_argument('--tmap-name', type=str, default=TMAP_NAME, help='Name for the tmap files that will be generated.')
    parser.add_argument('--fp', type=str, default='mhfp', help='Fingerprint to use. Supported {mhfp, morgan, mapc, map4, mqn}')
    parser.add_argument('--n-jobs', type=int, default=N_JOBS, help="Number of CPU cores to use")
    return parser.parse_args()

def setup_logging(log_level, log_output):
    if log_output == True:
        logging.basicConfig(level=getattr(logging, log_level.upper()), format=LOGGING_FORMAT, filename=LOG_FILE_PATH) # saving logs to file
    else: 
        logging.basicConfig(level=getattr(logging, log_level.upper()), format=LOGGING_FORMAT, stream=sys.stderr)  # Direct logs to stderr (console)
    logging.info("Logging initialized")

def main() -> None:
    args = parse_arguments()

    # Set up logging based on the parsed arugments or config defaults
    setup_logging(args.log_level, args.log)

    # logging.info(f"Processing file: {args.data_file}")  
    # logging.info(f"Output directory: {args.output_dir}")
    logging.info(f"Using K={TMAP_K} (number of neighbors)")
    logging.info(f"Using {args.n_jobs} CPU cores") 

    # Check for TMAP level
    if not args.l:
        raise ValueError("No TMAP level selected. With flag --l pass 'primary' for a primary TMAP or provide a cluster_id in the format 'int_int_int' for a secondary TMAP")

    if args.l == 'primary':
        # This line of code generates a simple TMAP
        # All configuration should be done passing the arguments either with config.py file or args.parser. 
        # Generate representative cluster. 

        tmap_generator = TmapGenerator(INPUT_TMAP_PATH, fingerprint_type=args.fp,categ_cols=['cluster_id'], output_name=args.tmap_name)
        tmap_generator.tmap_little()

    else:
            # Validate secondary TMAP label format
            if re.match(r"^\d+_\d+_\d+$", args.l):
                # Secondary TMAP processing logic        
                bin = args.l.split('_')[0]
                cluster_path = os.path.join(CLUSTER_DATA_PATH, f'cluster{bin}.csv')

                tmap_generator = TmapGenerator(cluster_path, fingerprint_type=args.fp)
                tmap_generator.generate_cluster_tmap(args.l)
            else:
                logging.error("Invalid cluster_id format. Expected format: 'int_int_int' (e.g., 1_10_23 or 10_11_1)")
                raise ValueError(f"Invalid cluster_id format. Please provide a cluster_id in the format 'int_int_int'. Instead got: {args.l}")



if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    start_time = time.time()
    main()
    logging.info("TMAP successfully generated")

    # Define paths
    source_pattern = os.path.join('/home/afloresep/work/chelombus/backend', '*.html')
    destination_dir = os.path.join('/home/afloresep/work/chelombus/backend', 'static')
    os.makedirs(destination_dir, exist_ok=True)

    logging.info(f"Source pattern for HTML files: {source_pattern}")
    logging.info(f"Destination directory: {destination_dir}")

    # Move HTML files
    for file_path in glob.glob(source_pattern):
        logging.info(f"Moving HTML file: {file_path} to {destination_dir}")
        shutil.move(file_path, destination_dir)

    # Move JavaScript files
    source_pattern_js = os.path.join('/home/afloresep/work/chelombus/backend', '*.js')
    for file_path in glob.glob(source_pattern_js):
        logging.info(f"Moving JS file: {file_path} to {destination_dir}")
        shutil.move(file_path, destination_dir)

    end_time = time.time()
    logging.info("All files moved successfully")
    print(f"Total execution time: {(end_time - start_time)/60:.2f} minutes")