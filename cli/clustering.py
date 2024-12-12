# CLI for clustering using TDigest and Spark 
# CLI for fingerprint calculation on billions of molecules
import sys
import pandas as pd
import logging 
import os
import pickle
from tdigest import TDigest
from pydantic import ValidationError
from api.utils.log_setup import setup_logging
from api.utils.common_arg_parser import common_arg_parser
from api.utils.config_loader import load_config
from api.utils.helper_functions import process_input, FileProgressTracker

def main() -> None:
    parser = common_arg_parser(description="Clustering from PCA using TDigest")
    parser.add_argument('--data-path', help="Input data file or directory.")
    parser.add_argument('--output-path', help="Directory to save buckets and tdigest model.")
    parser.add_argument("--model", type=str, default=None, help="Path to the tdigest model. This avoids batch_update")
    parser.add_argument("--save-model", type=bool, default=True, help="Save TDigest model. Default True")
    args = parser.parse_args() 

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
    logging.info(f"TDigest Model loaded: {config.TDIGEST_MODEL}")

    # This is just to ensure that in case a file is provided as input instead of a folder, Progress bar also works
    if os.path.isdir(config.DATA_PATH):
        total_files = len(os.listdir(config.DATA_PATH))
    else:
        total_files = 1 

    if args.model == None: 
        digest_model = TDigest()
            
        with FileProgressTracker(description="Calculating Fingerprints", total_files=total_files) as tracker:
            for file_path in process_input(config.DATA_PATH):
                parquet_dataframe = pd.read_parquet(file_path)
                digest_model.batch_update(parquet_dataframe['PCA_1'].to_numpy())
                tracker.update_progress()

        tdigest_model_path = os.path.join(config.DATA_PATH, 'tdigest_model.pkl')
        with open(tdigest_model_path, 'wb') as f:
            pickle.dump(digest_model, f)
            logging.info("TDigest model saved: tdigest_model.pkl")
    else: 
        with open(args.model, 'rb') as f: 
            pickle.load(f)


