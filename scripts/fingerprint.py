# CLI for fingerprint calculation on billions of molecules

# Script to run Fingerprint calculations on multiple files 

import os
import sys
import time
import logging 
import argparse
from pathlib import Path
import numpy as np
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from sklearn.decomposition import IncrementalPCA
from memory_profiler import profile
import pandas as pd
import os

from chelombus.data_handler import DataHandler

def process_input(input_path):
    if os.path.isdir(input_path):
        # Process all files in the directory
        for file in os.listdir(input_path):
            file_path = os.path.join(input_path, file)
            if os.path.isfile(file_path):  # Optional: filter by file extensions
                print(f"Processing file: {file_path}")
                yield file_path
    elif os.path.isfile(input_path):
        # Process a single file
        print(f"Processing file: {input_path}")
        yield input_path
    else:
        raise ValueError(f"Invalid input path: {input_path}")

# Example usage in a CLI script
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run dynamic_tmap pipeline.")
    parser.add_argument('--input-path', required=True, help="Input file or directory.")
    parser.add_argument('--output-dir', required=True, help="Directory to save results.")
    args = parser.parse_args()

    for file_path in process_input(args.input_path):
        # Process the file (e.g., calculate fingerprints)
        data_handler = DataHandler(file_path, chunksize=1000)
        # ... continue with processing ...

# Append the parent directory to sys.path to import modules from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import configurations and modules
from chelombus.utils.config import (DATA_FILE_PATH, OUTPUT_FILE_PATH, CHUNKSIZE, PCA_N_COMPONENTS,
                    LOGGING_LEVEL, LOGGING_FORMAT, LOG_FILE_PATH, N_JOBS, STEPS_LIST)
from chelombus.data_handler import DataHandler
from chelombus.fingerprint_calculator import FingerprintCalculator
from chelombus.output_generator import OutputGenerator 
from chelombus.dimensionality_reducer import DimensionalityReducer, get_percentiles

def parse_arguments():
    parser = argparse.ArgumentParser(description="Process fingerprints with flexible options.")
    
    # Optional arguments to override config settings
    parser.add_argument('--data-file', type=str, default=DATA_FILE_PATH, help="Input data file path.")
    parser.add_argument('--output-dir', type=str, default=OUTPUT_FILE_PATH, help="Output data directory.")
    parser.add_argument('--chunksize', type=int, default=CHUNKSIZE, help="Chunk size for loading data.")
    parser.add_argument('--pca-components', type=int, default=PCA_N_COMPONENTS, help="Number of PCA components.")
    parser.add_argument('--log-level', type=str, default=LOGGING_LEVEL,
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help="Logging verbosity level.")
    parser.add_argument('--n-jobs', type=int, default=N_JOBS, help="Number of CPU cores to use.")
    parser.add_argument('--resume', type=int, default=0, help="Resume from a specific chunk.")
    parser.add_argument('--log', type=bool, default=False, help="Saving logs to output.log file. Default False")
    return parser.parse_args()

def setup_logging(log_level, log_output):
    if log_output == True:

        logging.basicConfig(level=getattr(logging, log_level.upper()), format=LOGGING_FORMAT, filename=LOG_FILE_PATH) # saving logs to file
    
    else: 
        logging.basicConfig(level=getattr(logging, log_level.upper()), format=LOGGING_FORMAT, stream=sys.stderr)  # Direct logs to stderr (console)
    
    logging.info("Logging initialized")

def main() -> None:
    args = parse_arguments()

    # Set up logging based on the parsed arguments or config defaults
    setup_logging(args.log_level, args.log)
    
    logging.info(f"Processing file: {args.data_file}")
    logging.info(f"Output directory: {args.output_dir}")
    logging.info(f"Chunk size: {args.chunksize}")
    logging.info(f"Number of PCA components: {args.pca_components}")
    logging.info(f"Using {args.n_jobs} CPU cores")
    
    assert args.pca_components == len(STEPS_LIST), "STEPS_LIST should be same lenght as number of PCA_COMPONENTS" 

    # Initialize classes
    data_handler = DataHandler(args.data_file, args.chunksize)
       
    # Load data in chunks
    data_chunks, total_chunks = data_handler.load_data()

    # Process chunks with tqdm progress bar
    start = time.time()

    for idx, chunk in enumerate(tqdm(data_chunks, total= total_chunks, desc=f"Loading chunk and calculating its fingerprints")):
        data_handler.process_chunk(idx, chunk, args.output_dir)
        del idx, chunk

    end = time.time()    
    logging.info(f"Preprocessing of data took: {(end - start)/59:.2f} minutes")