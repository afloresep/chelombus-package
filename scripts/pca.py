#  Script to run iPCA in fingerprints.
import sys
import time
import logging 
import joblib
from pydantic import ValidationError
import tqdm 
import pandas as pd 
import os
from chelombus.utils.log_setup import setup_logging
from chelombus.utils.common_arg_parser import common_arg_parser
from chelombus.utils.config_loader import load_config
from chelombus.utils.helper_functions import process_input, TimeTracker
from chelombus.data_handler import DataHandler
from chelombus.output_generator import OutputGenerator
from sklearn.decomposition import IncrementalPCA
import gc

# Example usage in a CLI script
def main() -> None:
   parser = common_arg_parser(description="Calculate iPCA from fingerprints")
   parser.add_argument('--input-path', help="Input file or directory.")
   parser.add_argument('--output-dir', help="Directory to save fingerprints.")
   parser.add_argument("--chunksize", type=int, help="Override config chunksize.")
   parser.add_argument('--pca-components', type=int, help="Number of PCA components.")
   parser.add_argument('--header', type=int, help='Row number(s) containing column labels and marking the start of the data (zero-indexed). Default header is first row (header=0)')
   parser.add_argument('--smiles-col-idx', type=int, help='Column index for the smiles values in the input data. Default is first column (0)')
   parser.add_argument('--resume-chunk', type=int, default=0, help="Resume from a specific chunk.")
   parser.add_argument('--ipca-model', default=None, type=str, help='Load a PCA model using joblib to avoid the fitting and directly transform the result')
   args = parser.parse_args() 

   #Set up logging
   setup_logging(args.log_level)

   # Load configuration
   try:
      config = load_config(args.config)
   except ValidationError as e:
      logging.error(f"COnfiguration error: {e}")
      sys.exit(1)

   # Override configuration with CLI args if provided
   config = config.copy(update={
   "DATA_PATH":args.input_path or config.DATA_PATH,
   "OUTPUT_PATH":args.output_dir or config.OUTPUT_PATH,
   "CHUNKSIZE":args.chunksize or config.CHUNKSIZE,
   "PCA_N_COMPONENTS": args.pca_components or config.PCA_N_COMPONENTS,
   "N_JOBS": args.n_jobs or config.N_JOBS,
   }) 

   os.makedirs(config.OUTPUT_PATH, exist_ok=True)

   logging.info(f"Input path: {config.DATA_PATH}")
   logging.info(f"Output directory: {config.OUTPUT_PATH}")
   logging.info(f"Chunk size: {config.CHUNKSIZE}")
   logging.info(f"Using {config.N_JOBS} CPU cores")
   logging.info(f"Number of PCA Components: {config.PCA_N_COMPONENTS}")

   start = time.time()

   if args.ipca_model != None:
      try:
         ipca = joblib.load(args.ipca_model)
      except Exception as e:
         logging.error(f"iPCA model {args.ipca_model} could not be loaded. Error {e}") 
         sys.exit(1)
   else:
      ipca = IncrementalPCA(n_components=config.PCA_N_COMPONENTS) 
      for file_path in process_input(config.DATA_PATH):
         # TODO: Think best way to solve when user has different col_index and wants to change it. 
         data_handler = DataHandler(file_path=file_path, chunksize=config.CHUNKSIZE, smiles_col_index=1, header=None) 
         output_gen = OutputGenerator()

         total_chunks = data_handler.get_total_chunks()

         start = time.time()
         for idx in tqdm(range(args.resume_chunk,total_chunks), desc="Loading Fingerprints and iPCA partial fitting"):
            try:
               fp_chunk_path = os.path.join(config.OUTPUT_PATH, f'batch_parquet/fingerprints_chunk_{idx}.parquet') 
               df_fingerprints = pd.read_parquet(fp_chunk_path, engine="pyarrow")

               fingerprints_columns = df_fingerprints.drop(columns=['smiles']).values
               ipca.partial_fit(fingerprints_columns)

               del df_fingerprints, fingerprints_columns
               gc.collect()
            except Exception as e: 
               logging.error(f"Error during PCA fitting for chunk {idx}: {e}", exc_info=True)
         end = time.time()
         logging.info(f"iPCA fitting done in {config.DATA_PATH} took {int((end - start) // 3600)} hours, {int(((end - start) % 3600) // 60)} minutes, and {((end - start) % 60):.2f} seconds")

   for file_path in process_input(config.DATA_PATH):
      data_handler = DataHandler(file_path=file_path, chunksize=config.CHUNKSIZE, smiles_col_index=1, header=None) # TODO: Think best way to solve when user has different col_index and wants to change it.
      total_chunks = data_handler.get_total_lines()
      start = time.time()

      for idx in tqdm(range(args.resume_chunk, total_chunks), desc='iPCA transform and saving results'):
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

      end = time.time()
      logging.info(f"iPCA fitting done in {config.DATA_PATH} took {int((end - start) // 3600)} hours, {int(((end - start) % 3600) // 60)} minutes, and {((end - start) % 60):.2f} seconds")

   end = time.time()
   logging.info(f"iPCA calculations for {config.DATA_PATH} took {int((end - start) // 3600)} hours, {int(((end - start) % 3600) // 60)} minutes, and {((end - start) % 60):.2f} seconds")
