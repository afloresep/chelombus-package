# CLI for fingerprint calculation on billions of molecules
import sys
import time
import logging 
from pydantic import ValidationError
import tqdm 
import pandas as pd 
import os
from api.utils.log_setup import setup_logging
from api.utils.common_arg_parser import common_arg_parser
from api.utils.config_loader import load_config
from api.utils.helper_functions import process_input, TimeTracker , FileProgressTracker
from api.data_handler import DataHandler  

def main() -> None:
   parser = common_arg_parser(description="Calculate iPCA from fingerprints")
   parser.add_argument('--data-path', help="Input data file or directory.")
   parser.add_argument('--output-path', help="Directory to save fingerprints.")
   parser.add_argument("--chunksize", type=int, help="Override config chunksize.")
   parser.add_argument('--resume-chunk', type=int, default=0, help="Resume from a specific chunk.")
   parser.add_argument('--n-jobs', type=int, help='Number of CPU cores to be used. Default uses all available CPU cores')
   args = parser.parse_args() 

   start = time.time()
   
   #Set up logging
   setup_logging(args.log_level)

   # Load configuration
   try:
      config = load_config(args.config)
   except ValidationError as e:
      logging.error(f"Configuration error: {e}")
      sys.exit(1)

   # Override configuration with CLI args if provided
   config = config.copy(update={
   "DATA_PATH":args.data_path or config.DATA_PATH,
   "OUTPUT_PATH":args.output_path or config.OUTPUT_PATH,
   "CHUNKSIZE":args.chunksize or config.CHUNKSIZE,
   "N_JOBS": args.n_jobs or config.N_JOBS,
   }) 

   os.makedirs(config.OUTPUT_PATH, exist_ok=True)

   logging.info(f"Input path: {config.DATA_PATH}")
   logging.info(f"Output directory: {config.OUTPUT_PATH}")
   logging.info(f"Chunksize: {config.CHUNKSIZE}")
   logging.info(f"Using {config.N_JOBS} CPU cores")

   # This is just to ensure that in case a file is provided as input instead of a folder, Progress bar also works
   if os.path.isdir(config.DATA_PATH):
      total_files = len(os.listdir(config.DATA_PATH))
   else:
      total_files = 1 

   with FileProgressTracker(description="Calculating Fingerprints", total_files=total_files) as tracker:
    for file_path in process_input(config.DATA_PATH):
        data_handler = DataHandler(file_path, chunksize=config.CHUNKSIZE)
        data_chunks, total_chunks= data_handler.load_data()

        for idx, chunk in enumerate(tqdm(data_chunks, total= total_chunks, desc=f"Loading chunk and calculating its fingerprints")):
            data_handler.process_chunk(idx, chunk, config.OUTPUT_PATH)
            del idx, chunk
        tracker.update_progress()
   end = time.time()
   logging.info(f"Fingerprint calculation for {config.DATA_PATH} took: {TimeTracker.format_time(end -start)}")