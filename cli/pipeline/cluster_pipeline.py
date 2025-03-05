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
import joblib 

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
   parser.add_argument('--data-path', nargs='+', help="Input data file or directory.")
   parser.add_argument('--output-path', help="Directory to save fingerprints.")
   parser.add_argument("--chunksize", type=int, help="Override config chunksize.")
   parser.add_argument("--n_components", type=int, help="Number of PCA components to reduce to")
   parser.add_argument('--resume-chunk', type=int, default=0, help="Resume from a specific chunk.")
   parser.add_argument('--ipca-model', default=None, type=str, help='Load a PCA model using joblib to avoid the fitting and directly transform the result')
   parser.add_argument('--save-model', default=True, type=str, help='Save the iPCA model after fitting. Default is True')
   parser.add_argument('--smiles-col-idx', type=int, default=0, help='Column index for SMILES. Indicates where \
                       the SMILES are in the dataset. Default is 0. ')
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
   "PCA_N_COMPONENTS": args.n_components or config.PCA_N_COMPONENTS,
   "SMILES_COL_IDX": args.smiles_col_idx or config.SMILES_COL_IDX,
   "IPCA_MODEL": args.ipca_model or config.IPCA_MODEL
   }) 

   os.makedirs(config.OUTPUT_PATH, exist_ok=True)

   logging.info(f"Input path: {config.DATA_PATH}")
   logging.info(f"Output directory: {config.OUTPUT_PATH}")
   logging.info(f"Chunksize: {config.CHUNKSIZE}")
   logging.info(f"Using {config.N_JOBS} CPU cores")

   output_gen = OutputGenerator()
   start = time.time()
   
   #########################################################
   #  FINGERPRINT CALCULATIONS
   #########################################################

   # Within the output path we create a folder to store the fingerprints
   fp_folder_path = os.path.join(config.OUTPUT_PATH, 'fingerprints')
   os.makedirs(fp_folder_path, exist_ok=True)

   # For every `file_path` in the input data path we calculate the fp. 
   # If config.DATA_PATH is a file and not a folder `process_input` will deal with it. Works either way
   for file_path in process_input(config.DATA_PATH):
      logging.info(f"Processing:  {file_path}")
      data_handler = DataHandler(file_path, chunksize=config.CHUNKSIZE,smiles_col_index=config.SMILES_COL_IDX)
      try:
         data_chunks, total_chunks= data_handler.load_data()
      except Exception as e:
         logging.error(f"Exception ocurred when processing {file_path}, raised {e}")
         break
      for idx, chunk in enumerate(tqdm(data_chunks, total= total_chunks,
                                       desc=f"Loading chunk and calculating its fingerprints",
                                        unit= "chunk")):
         data_handler.process_chunk(idx, chunk, fp_folder_path)
         if idx == 10:
             break
         del idx, chunk
   end = time.time()
   logging.info(f"Fingerprint calculation for {config.DATA_PATH} took: {format_time(end -start)}")


   #########################################################
   # iPCA FITTING 
   #########################################################
   # First check whether there's a iPCA model passed -to not have to fit again-
   if config.IPCA_MODEL == None:
        ipca = IncrementalPCA(n_components=config.PCA_N_COMPONENTS)   
        logging.info("NO PCA MODEL")
        # Now all the fingerprints files were saved on config.OUTPUT_PATH. So we will use all files on 
        # config.OUTPUT_PATH/fingerprints/ and use them to fit the iPCA model

        for file_path in tqdm(os.listdir(fp_folder_path),desc="Loading Fingerprints and iPCA partial fitting"):
                try:
                    fp_chunk_path = os.path.join(fp_folder_path,file_path)
                    # Read the fingerprint chunk 
                    df_fingerprints = pd.read_parquet(fp_chunk_path, engine="pyarrow")
                    # Drop smiles. This leaves us with only the FP columns     
                    fingerprints_columns = df_fingerprints.drop(columns=['smiles']).values

                    ipca.partial_fit(fingerprints_columns)

                    del df_fingerprints, fingerprints_columns 
                    gc.collect()

                except Exception as e:
                    logging.error(f"Error during PCA fitting for chunk {fp_chunk_path}: {e}", exc_info=True)

        # Load model if args.save_model == True
        if args.save_model: 
            joblib.dump(ipca, os.path.join(config.OUTPUT_PATH, 'ipca-model.joblib'))
            logging.info("Model saved")

   # If a ipca-model was passed, then load it
   else:
       try:
           ipca = joblib.load(config.IPCA_MODEL)   
       except Exception as e:
           logging.error(f"iPCA model {config.IPCA_MODEL} could not be loaded. Error {e}")
           sys.exit(1)

   #########################################################
   # iPCA TRANSFORM (FINGERPRINT VECTOR-> PCA VECTOR) 
   #########################################################

   # Same as for fingerprints. Create a separated folder in config.OUTPUT_PATH
   # to store the output parquet files of 3D PCA valuesH

   pca_vectors_folder_path = os.path.join(config.OUTPUT_PATH, 'pca_vectors')
   os.makedirs(pca_vectors_folder_path, exist_ok=True)

   for file_path in tqdm(os.listdir(fp_folder_path),desc="Trasnforming FP into 3D vectors"):
            try:
                file_name = file_path.split('.')[0]
                fp_chunk_path = os.path.join(fp_folder_path, file_path)
                # Read the fingerprint chunk 
                df_fingerprints = pd.read_parquet(fp_chunk_path, engine="pyarrow")
                # Drop smiles. This leaves us with only the FP columns     
                fingerprints_columns = df_fingerprints.drop(columns=['smiles']).values

                coordinates = ipca.transform(df_fingerprints.drop(columns=['smiles']).values) # -> np.array shape (chunksize, n_pca_comp)
                output_gen.batch_to_multiple_parquet(file_name, coordinates,df_fingerprints['smiles'].to_list(), pca_vectors_folder_path)

                del df_fingerprints, coordinates
                gc.collect()

            except Exception as e:
                logging.error(f"Error during PCA fitting for chunk {fp_chunk_path}: {e}", exc_info=True)

               # os.remove(fp_chunk_path) 

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
