# CLI for fingerprint calculation on billions of molecules

import time
import logging 
import tqdm 
import os
from chelombus.data_handler import DataHandler
from chelombus.utils.log_setup import setup_logging
from chelombus.utils.common_arg_parser import common_arg_parser
from chelombus.utils.config_loader import load_config
from chelombus.utils.helper_functions import process_input, TimeTracker

# Example usage in a CLI script
def main() -> None:
    parser = common_arg_parser(description="Calculate fingerprints")
    parser.add_argument('--input-path', help="Input file or directory.")
    parser.add_argument('--output-dir', help="Directory to save fingerprints.")
    parser.add_argument("--fingerprint-type", choices=["mqn", "morgan", "mhfp", "mapc"], default="mqn",
                        help="Type of fingerprint to calculate.")
    parser.add_argument("--fp-size", type=int, default=1024, help="Size of fingerprint vectors.")
    parser.add_argument("--chunksize", type=int, help="Override config chunksize.")
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
    config["N_JOBS"] = args.n_jobs or config.get("N_JOBS")
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    input_path = config["DATA_PATH"]
    output_dir = config["OUTPUT_PATH"]
    chunksize = config["CHUNKSIZE"]
    n_jobs = config["N_JOBS"]

    logging.info(f"Input path: {input_path}")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Chunk size: {chunksize}")
    logging.info(f"Using {n_jobs} CPU cores")

    with TimeTracker(description='Fingerprints calculations'):
        for file_path in process_input(input_path):
            logging.info(f"Processing file: {file_path}")
            # Process the file (e.g., calculate fingerprints)
            data_handler = DataHandler(file_path, chunksize=chunksize)
            data_chunks, total_chunks = data_handler.load_data()

            start = time.time()
            for idx, chunk in enumerate(tqdm(data_chunks, total= total_chunks, desc=f"Loading chunk and calculating its fingerprints")):
                data_handler.process_chunk(idx, chunk, output_dir)
                del idx, chunk
            logging.info(f"Calculations for {file_path} took {int((end - start) // 3600)} hours, {int(((end - start) % 3600) // 60)} minutes, and {((end - start) % 60):.2f} seconds")
  

    # Process chunks with tqdm progress bar

    end = time.time()    
    logging.info(f"Fingerprints calculations for {input_path} took {int((end - start) // 3600)} hours, {int(((end - start) % 3600) // 60)} minutes, and {((end - start) % 60):.2f} seconds")
