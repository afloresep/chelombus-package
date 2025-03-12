#  Script to run iPCA in fingerprints.
import sys
import time
import logging 
import joblib
from tqdm import tqdm 
from pydantic import ValidationError
import pandas as pd 
import os
from api.utils.log_setup import setup_logging
from api.utils.common_arg_parser import common_arg_parser
from api.utils.config_loader import load_config
from api.utils.helper_functions import format_time
from api.output_generator import OutputGenerator
from api.output_generator import OutputGenerator 
from sklearn.decomposition import IncrementalPCA
import gc

# Append the parent directory to sys.path to import modules from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def main() -> None:
   parser = common_arg_parser(description="Calculate iPCA from fingerprints files")
   parser.add_argument('--data-path', help="Directory containing the fingerprint files")
   parser.add_argument('--output-path', help="Directory to save the PCA vectors.")
   parser.add_argument('--pca-components', type=int, help="Number of PCA components.")
   parser.add_argument('--resume-chunk', type=int, default=0, help="Resume from a specific chunk.")
   parser.add_argument('--ipca-model', default=None, type=str, help='Load a PCA model using joblib to avoid the fitting and directly transform the result')
   parser.add_argument('--save-model', default=False, type=str, help='Save the iPCA model after fitting. Default is False')
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
   "PCA_N_COMPONENTS": args.pca_components or config.PCA_N_COMPONENTS,
   "N_JOBS": args.n_jobs or config.N_JOBS,
   "IPCA_MODEL": args.ipca_model or config.IPCA_MODEL
   }) 

   os.makedirs(config.OUTPUT_PATH, exist_ok=True)

   logging.info(f"Input path: {config.DATA_PATH}")
   logging.info(f"Output directory: {config.OUTPUT_PATH}")
   logging.info(f"IPCA model: {config.IPCA_MODEL}")
   logging.info(f"Using {config.N_JOBS} CPU cores")
   logging.info(f"Number of PCA Components: {config.PCA_N_COMPONENTS}")


   output_gen = OutputGenerator()
   start = time.time()

    #########################################################
   #                   iPCA FITTING                         #
   #########################################################
   # First check whether there's a iPCA model passed -to not have to fit again-
   if config.IPCA_MODEL == None:
        ipca = IncrementalPCA(n_components=config.PCA_N_COMPONENTS)   
        logging.info("NO PCA MODEL")
        # Now all the fingerprints files were saved on config.OUTPUT_PATH. So we will use all files on 
        # config.OUTPUT_PATH/fingerprints/ and use them to fit the iPCA model

        for file_path in tqdm(os.listdir(config.DATA_PATH),desc="Loading Fingerprints and iPCA partial fitting"):
                try:
                    fp_chunk_path = os.path.join(config.DATA_PATH,file_path) # -> full path 
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


   end = time.time()
   logging.info(f"iPCA fitting for {config.DATA_PATH} took {format_time(end - start)}")

   #########################################################
   #    iPCA TRANSFORM (FINGERPRINT VECTOR-> PCA VECTOR)   # 
   #########################################################

   # Create a separated folder in config.OUTPUT_PATH
   # to store the output parquet files of 3D PCA values

   pca_vectors_folder_path = os.path.join(config.OUTPUT_PATH, 'pca_vectors')
   os.makedirs(pca_vectors_folder_path, exist_ok=True)

   for file_path in tqdm(os.listdir(config.DATA_PATH),desc="Transforming FP into 3D vectors"):
            try:
                file_name = file_path.split('.')[0]
                fp_chunk_path = os.path.join(config.DATA_PATH, file_path)
                # Read the fingerprint chunk 
                df_fingerprints = pd.read_parquet(fp_chunk_path, engine="pyarrow")
                # Drop smiles. This leaves us with only the FP columns     
                fingerprints_columns = df_fingerprints.drop(columns=['smiles']).values

                coordinates = ipca.transform(df_fingerprints.drop(columns=['smiles']).values) # -> np.array shape (chunksize, n_pca_comp)
                output_gen.batch_to_multiple_parquet(file_name, coordinates,df_fingerprints['smiles'].to_list(), pca_vectors_folder_path)

                del df_fingerprints, coordinates
                gc.collect()

                #Remove the fingerprint input folder if indicated
                if args.remove == True: 
                    os.remove(fp_chunk_path)

            except Exception as e:
                logging.error(f"Error during PCA fitting for chunk {fp_chunk_path}: {e}", exc_info=True)


if __name__ == '__main__':
    start_time = time.time()
    # Run the main function
    main()
    # Manually invoke garbage collection
    gc.collect()
    # Clear all modules 
    sys.modules.clear()
    # Clear any cached variables or objects
    del main
    end_time = time.time()
    logging.info(f"Total execution time: {format_time(end_time - start_time)}")