import bz2
import time

# Define the file paths
compressed_file_path = '/home/afloresep/Downloads/Enamine_REAL_HAC_29_38_1.3B_Part_2_CXSMILES.cxsmiles.bz2'
uncompressed_file_path = '/home/afloresep/work/chelombus/data/Enamine_REAL_HAC_29_38_1.3B_Part_2_CXSMILES.cxsmiles'

# Define the number of chunks to process
chunksize = 110000
num_chunks_to_test = 200  # Number of chunks to process for benchmarking

def read_compressed_cxsmiles(file_path, chunksize):
    with bz2.open(file_path, 'rt') as f:
        chunk = []
        for idx, line in enumerate(f):
            chunk.append(line.strip())  # Strip newline characters
            if (idx + 1) % chunksize == 0:
                yield chunk
                chunk = []
                # Stop after processing the desired number of chunks
                if idx // chunksize + 1 >= num_chunks_to_test:
                    break
        # Yield any remaining lines
        if chunk:
            yield chunk

def read_uncompressed_cxsmiles(file_path, chunksize):
    with open(file_path, 'r') as f:
        chunk = []
        for idx, line in enumerate(f):
            chunk.append(line.strip())  # Strip newline characters
            if (idx + 1) % chunksize == 0:
                yield chunk
                chunk = []
                # Stop after processing the desired number of chunks
                if idx // chunksize + 1 >= num_chunks_to_test:
                    break
        # Yield any remaining lines
        if chunk:
            yield chunk

# Benchmark the compressed file
start_time = time.time()
for chunk in read_compressed_cxsmiles(compressed_file_path, chunksize):
    # Simulate processing by iterating over the chunk
    pass
compressed_time = time.time() - start_time

# Benchmark the uncompressed file
start_time = time.time()
for chunk in read_uncompressed_cxsmiles(uncompressed_file_path, chunksize):
    # Simulate processing by iterating over the chunk
    pass
uncompressed_time = time.time() - start_time

print(f"Time taken for compressed file (bz2): {compressed_time:.2f} seconds")
print(f"Time taken for uncompressed file: {uncompressed_time:.2f} seconds")

"""
output:

Time taken for compressed file (bz2): 64.01 seconds
Time taken for uncompressed file: 3.78 seconds

"""
