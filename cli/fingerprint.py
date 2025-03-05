# CLI for fingerprint calculation on billions of molecules
import sys
import time
import logging 
from pydantic import ValidationError
from tqdm import tqdm 
import os
from api.utils.log_setup import setup_logging
from api.utils.common_arg_parser import common_arg_parser
from api.utils.config_loader import load_config
from api.utils.helper_functions import process_input, format_time, TimeTracker , ProgressTracker
from api.data_handler import DataHandler  

def main() -> None:
   parser = common_arg_parser(description="Calculate fingerprints from SMILES")
   parser.add_argument('--data-path', help="Input data file or directory.")
   parser.add_argument('--output-path', help="Directory to save fingerprints.")
   parser.add_argument("--chunksize", type=int, help="Override config chunksize.")
   parser.add_argument('--resume-chunk', type=int, default=0, help="Resume from a specific chunk.")
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


   for file_path in process_input(config.DATA_PATH):
      logging.info("Processing: ", file_path)
      data_handler = DataHandler(file_path, chunksize=config.CHUNKSIZE,smiles_col_index=1)
      try:
         data_chunks, total_chunks= data_handler.load_data()
      except Exception as e:
         logging.error(f"Exception ocurred when processing {file_path}, raised {e}")
         break
      for idx, chunk in enumerate(tqdm(data_chunks, total= total_chunks, desc=f"Loading chunk and calculating its fingerprints")):
         data_handler.process_chunk(idx, chunk, config.OUTPUT_PATH)
         del idx, chunk
   end = time.time()
   logging.info(f"Fingerprint calculation for {config.DATA_PATH} took: {format_time(end -start)}")
   

if __name__=="__main__":
   main()