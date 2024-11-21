import os
import time
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pickle
from config import DATA_FILE_PATH, OUTPUT_FILE_PATH, CHUNKSIZE, N_JOBS
from tqdm import tqdm  # Import tqdm
from src.data_handler import DataHandler, get_total_chunks
from src.fingerprint_calculator import FingerprintCalculator
from src.output_generator import OutputGenerator
import time


import os
import pickle
import time
import matplotlib.pyplot as plt
from tqdm import tqdm

# Define the list of chunksizes to test

CHUNKSIZES_TO_TEST = range(80000, 255000, 5000)

# Initialize classes
output_gen = OutputGenerator()
fp_calculator = FingerprintCalculator()


def run_chunks_with_chunksize(chunksize, data_handler, num_chunks_to_run=1):

    """
    Run a few chunks with a specific chunksize and return the average processing time.
    """
    data_chunks, total_chunks = data_handler.load_data()

    processing_times = []
    num_chunks = 0
    for idx, chunk in enumerate(tqdm(data_chunks, total=num_chunks_to_run, desc=f"Processing Chunks (Chunksize={chunksize})")):
        if num_chunks >= num_chunks_to_run:
            break
        start = time.time()
        num_chunks += 1

        # Extract smiles and features from chunk
        smiles_list, features = data_handler.extract_smiles_and_features(chunk)

        # Calculate fingerprints
        fingerprints = fp_calculator.calculate_fingerprints(smiles_list)

        # Free space
        del smiles_list, features, fingerprints

        end = time.time()
        processing_times.append(end - start)

    # Calculate the average processing time for the chunks
    avg_processing_time = sum(processing_times) / len(processing_times)
    total_time = total_chunks * avg_processing_time
    return total_time

def main():
    avg_times = []

    # Test each chunksize
    for chunksize in CHUNKSIZES_TO_TEST:
        data_handler = DataHandler(DATA_FILE_PATH, chunksize)
        time_total = run_chunks_with_chunksize(chunksize, data_handler, num_chunks_to_run=3)
        avg_times.append(time_total/3600)
        print(f'Average time for chunksize {chunksize}: {time_total/3600} hours')

    # Plot the results
    plt.figure(figsize=(10, 6))
    plt.plot(CHUNKSIZES_TO_TEST, avg_times, marker='o')
    plt.xlabel('Chunk Size')
    plt.ylabel('Average Time per Chunk (hours)')
    plt.title('Chunk Size vs Average Processing Time')
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    main()