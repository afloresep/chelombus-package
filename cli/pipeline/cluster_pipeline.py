# File to run the Clustering pipeline
import os
import sys
import time
import logging 
import argparse
from tqdm import tqdm
from sklearn.decomposition import IncrementalPCA
import gc
import pandas as pd

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

# Append the parent directory to sys.path to import modules from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from api.data_handler import DataHandler
from api.fingerprint_calculator import FingerprintCalculator
from api.output_generator import OutputGenerator 


def main() -> None:
   parser = common_arg_parser(description="Calculate fingerprints from SMILES")
   parser.add_argument('--data-path', help="Input data file or directory.")
   parser.add_argument('--output-path', help="Directory to save fingerprints.")
   parser.add_argument("--chunksize", type=int, help="Override config chunksize.")
   parser.add_argument("--n_components", type=int, help="Number of PCA components to reduce to")
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
   "PCA_N_COMPONENTS": args.n_components or config.PCA_N_COMPONENTS
   }) 

   os.makedirs(config.OUTPUT_PATH, exist_ok=True)

   logging.info(f"Input path: {config.DATA_PATH}")
   logging.info(f"Output directory: {config.OUTPUT_PATH}")
   logging.info(f"Chunksize: {config.CHUNKSIZE}")
   logging.info(f"Using {config.N_JOBS} CPU cores")

   # Initialize classes
   data_handler = DataHandler(config.DATA_PATH, config.CHUNKSIZE)
   output_gen = OutputGenerator()
       
   # Load data in chunks
   data_chunks, total_chunks = data_handler.load_data()

   # Process chunks with tqdm progress bar
   start = time.time()

   for idx, chunk in enumerate(tqdm(data_chunks, total= total_chunks, desc=f"Loading chunk and calculating its fingerprints")):
       data_handler.process_chunk(idx, chunk, config.OUTPUT_PATH)
       del idx, chunk

   end = time.time()    
   logging.info(f"Preprocessing of data took: {(end - start)/59:.2f} minutes")

   ipca = IncrementalPCA(n_components=config.PCA_N_COMPONENTS)  # Dimensions to reduce to

   # Incremental PCA fitting
   for idx in tqdm(range(total_chunks), desc="Loading Fingerprints and iPCA partial fitting"):
       try:
           fp_chunk_path = os.path.join(config.OUTPUT_PATH, f'batch_parquet/fingerprints_chunk_{idx}.parquet')
           df_fingerprints = pd.read_parquet(fp_chunk_path, engine="pyarrow")
       
           fingerprints_columns = df_fingerprints.drop(columns=['smiles']).values
           ipca.partial_fit(fingerprints_columns)

           del df_fingerprints, fingerprints_columns 
           gc.collect()

       except Exception as e:
           logging.error(f"Error during PCA fitting for chunk {idx}: {e}", exc_info=True)

   for idx in tqdm(range(total_chunks), desc='iPCA transform and saving results'):
           try:
               # Load fingerprint
               fp_chunk_path = os.path.join(config.OUTPUT_PATH, f'batch_parquet/fingerprints_chunk_{idx}.parquet')
               df_fingerprints  =  pd.read_parquet(fp_chunk_path)
               features = []
               coordinates = ipca.transform(df_fingerprints.drop(columns=['smiles']).values)  # -> np.array shape (chunk_size, n_pca_comp)

           # Output coordinates into a parquet file.
               output_gen.batch_to_multiple_parquet(idx, coordinates,df_fingerprints['smiles'].to_list(), features, config.OUTPUT_PATH)

               # Free memory space
               del df_fingerprints, coordinates, features 
               # os.remove(fp_chunk_path) 

           except Exception as e:
               logging.error(f"Error during data transformation for chunk {idx}: {e}", exc_info=True)

           
if __name__ == '__main__':
    start_time = time.time()
    # Run the main function
    main()
    # Manually invoke garbage collection
    gc.collect()
    # Clear all modules (optional but useful for memory cleanup)
    sys.modules.clear()
    # Clear any cached variables or objects
    del main
    end_time = time.time()
    logging.info(f"Total execution time: {(end_time - start_time)/60:.2f} minutes")
