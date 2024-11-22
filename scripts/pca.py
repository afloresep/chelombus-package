# Script to run iPCA in fingerprints.
import time
import logging 
import tqdm 
import pandas as pd 
import os
from chelombus.utils.logging import setup_logging
from chelombus.utils.common_arg_parser import common_arg_parser
from chelombus.utils.config_loader import load_config
from chelombus.utils.helper_functions import process_input
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
    parser.add_argument('--smiles_col_idx', type=int, help='Column index for the smiles values in the input data. Default is first column (0)')
    parser.add_argument('--resume', type=int, default=0, help="Resume from a specific chunk.")
    args = parser.parse_args() 

    #Set up logging
    setup_logging(args.log_level)

    # Load configuration
    config = load_config(args.config)
    logging.info("Configuration loaded successfully")

    # Override configuration with CLI args if provided
    config["DATA_PATH"] = args.input_path or config.get("DATA_PATH")
    config["OUTPUT_PATH"] = args.output_dir or config.get("OUTPUT_PATH")
    config["CHUNKSIZE"] = args.chunksize or config.get("CHUNKSIZE")
    config["PCA_N_COMPONENTS"] = args.pca_components or config.get("PCA_N_COMPONENTS")
    config["N_JOBS"] = args.n_jobs or config.get("N_JOBS")
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    input_path = config["DATA_PATH"]
    output_dir = config["OUTPUT_PATH"]
    chunksize = config["CHUNKSIZE"]
    n_jobs = config["N_JOBS"]
    pca_n_components = config["PCA_N_COMPONENTS"]

    logging.info(f"Input path: {input_path}")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Chunk size: {chunksize}")
    logging.info(f"Using {n_jobs} CPU cores")
    logging.info(f"Number of PCA Components: {pca_n_components}")

    start = time.time()

    ipca = IncrementalPCA(pca_n_components=pca_n_components) 
    for file_path in process_input(input_path):
       data_handler = DataHandler(file_path=file_path, chunksize=chunksize, smiles_col_index=1, header=None) # TODO: Think best way to solve when user has different col_index and wants to change it. 
       output_gen = OutputGenerator()

       total_chunks = data_handler.get_total_chunks()

       start = time.time()
       for idx in tqdm(range(args.resume,total_chunks), desc="Loading Fingerprints and iPCA partial fitting"):
         try:
            fp_chunk_path = os.path.join(output_dir, f'batch_parquet/fingerprints_chunk_{idx}.parquet') 
            df_fingerprints = pd.read_parquet(fp_chunk_path, engine="pyarrow")

            fingerprints_columns = df_fingerprints.drop(columns=['smiles']).values
            ipca.partial_fit(fingerprints_columns)

            del df_fingerprints, fingerprints_columns
            gc.collect()
         except Exception as e:
            logging.error(f"Error during PCA fitting for chunk {idx}: {e}", exc_info=True)
       end = time.time()
       logging.info(f"iPCA fitting done in {input_path} took {int((end - start) // 3600)} hours, {int(((end - start) % 3600) // 60)} minutes, and {((end - start) % 60):.2f} seconds")

    for file_path in process_input(input_path):
       data_handler = DataHandler(file_path=file_path, chunksize=chunksize, smiles_col_index=1, header=None) # TODO: Think best way to solve when user has different col_index and wants to change it.
       total_chunks = data_handler.get_total_lines(9)
       start = time.time()

       for idx in tqdm(range(args.resume, total_chunks), desc='iPCA transform and saving results'):
         try:
            # Load fingerprint
            fp_chunk_path = os.path.join(args.output_dir, f'batch_parquet/fingerprints_chunk_{idx}.parquet')
            df_fingerprints  =  pd.read_parquet(fp_chunk_path)
            features = []
            coordinates = ipca.transform(df_fingerprints.drop(columns=['smiles']).values)  # -> np.array shape (chunk_size, n_pca_comp)
 
            # Output coordinates into a parquet file.
            output_gen.batch_to_multiple_parquet(idx, coordinates,df_fingerprints['smiles'].to_list(), features, args.output_dir)
 
            # Free memory space
            del df_fingerprints, coordinates, features 
            # os.remove(fp_chunk_path) 
 
         except Exception as e:
            logging.error(f"Error during data transformation for chunk {idx}: {e}", exc_info=True)
 
       end = time.time()
       logging.info(f"iPCA fitting done in {input_path} took {int((end - start) // 3600)} hours, {int(((end - start) % 3600) // 60)} minutes, and {((end - start) % 60):.2f} seconds")

    end = time.time()
    logging.info(f"iPCA calculations for {input_path} took {int((end - start) // 3600)} hours, {int(((end - start) % 3600) // 60)} minutes, and {((end - start) % 60):.2f} seconds")
