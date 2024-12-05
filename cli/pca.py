#  Script to run iPCA in fingerprints.
import sys
import time
import logging 
import joblib
from pydantic import ValidationError
import tqdm 
import pandas as pd 
import os
from api.utils.log_setup import setup_logging
from api.utils.common_arg_parser import common_arg_parser
from api.utils.config_loader import load_config
from api.utils.helper_functions import process_input, TimeTracker , RAMAndTimeTracker, FileProgressTracker
from api.data_handler import DataHandler  
from api.output_generator import OutputGenerator
from sklearn.decomposition import IncrementalPCA
import gc

def main() -> None:
   parser = common_arg_parser(description="Calculate iPCA from fingerprints")
   parser.add_argument('--data-path', help="Input data file or directory.")
   parser.add_argument('--output-path', help="Directory to save fingerprints.")
   parser.add_argument("--chunksize", type=int, help="Override config chunksize.")
   parser.add_argument('--pca-components', type=int, help="Number of PCA components.")
   parser.add_argument('--header', type=int, help='Row number(s) containing column labels and marking the start of the data (zero-indexed). Default header is first row (header=0)')
   parser.add_argument('--smiles-col-idx', type=int, help='Column index for the smiles values in the input data. Default is first column (0)')
   parser.add_argument('--resume-chunk', type=int, default=0, help="Resume from a specific chunk.")
   parser.add_argument('--ipca-model', default=None, type=str, help='Load a PCA model using joblib to avoid the fitting and directly transform the result')
   parser.add_argument('--remove', default=False, type=bool, help='Remove fingerprints files after PCA reduction')
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
   "PCA_N_COMPONENTS": args.pca_components or config.PCA_N_COMPONENTS,
   "N_JOBS": args.n_jobs or config.N_JOBS,
   "IPCA_MODEL": args.ipca_model or config.IPCA_MODEL
   }) 

   os.makedirs(config.OUTPUT_PATH, exist_ok=True)

   logging.info(f"Input path: {config.DATA_PATH}")
   logging.info(f"Output directory: {config.OUTPUT_PATH}")
   logging.info(f"IPCA model: {config.IPCA_MODEL}")
   logging.info(f"Chunk size: {config.CHUNKSIZE}")
   logging.info(f"Using {config.N_JOBS} CPU cores")
   logging.info(f"Number of PCA Components: {config.PCA_N_COMPONENTS}")

   # This is just to ensure that in case a file is provided as input instead of a folder, Progress bar also works
   if os.path.isdir(config.DATA_PATH):
      total_files = len(os.listdir(config.DATA_PATH))
   else:
      total_files = 1 

   with FileProgressTracker(description="Fitting", total_files=total_files) as tracker:
      if config.IPCA_MODEL == None:
         ipca = IncrementalPCA(n_components=config.PCA_N_COMPONENTS) 
         for idx, file_path in enumerate(process_input(config.DATA_PATH)):
            # TODO: Think best way to solve when user has different col_index and wants to change it. 
            try:
               df_fingerprints = pd.read_parquet(file_path, engine="pyarrow")
               fingerprints_columns = df_fingerprints.drop(columns=['smiles']).values
               ipca.partial_fit(fingerprints_columns)

               del df_fingerprints, fingerprints_columns
               gc.collect()
               tracker.update_progress()
            except Exception as e: 
               logging.error(f"Error during PCA fitting for chunk {idx}: {e}", exc_info=True)

      else: 
         try:
            ipca = joblib.load(args.ipca_model)
         except Exception as e:
            logging.error(f"iPCA model {args.ipca_model} could not be loaded. Error {e}") 
            sys.exit(1)

   with FileProgressTracker(description="Transforming and saving PCA results", total_files=total_files) as tracker:
      for file_path in process_input(config.DATA_PATH):
         try:
            # Load fingerprint
            df_fingerprints  =  pd.read_parquet(file_path)
            features = []
            coordinates = ipca.transform(df_fingerprints.drop(columns=['smiles']).values)  # -> np.array shape (chunk_size, n_pca_comp)

            # Output coordinates into a parquet file.
            OutputGenerator.batch_to_multiple_parquet(idx, coordinates,df_fingerprints['smiles'].to_list(), features, config.OUTPUT_PATH)

            # Free memory space
            del df_fingerprints, coordinates, features 
            if args.remove:
               os.remove(file_path)
   
         except Exception as e:
               logging.error(f"Error during data transformation for chunk {idx}: {e}", exc_info=True)

   end = time.time()
   logging.info(f"iPCA calculations for {config.DATA_PATH} took {TimeTracker.format_time(end - start)}")
