#!/bin/bash

# Default values
INPUT_PATH="/mnt/samsung_2tb/enamine_db_10b_processed/pca_vectors/pca_vectors_output_file_0_fp_chunk_0.parquet"
OUTPUT_BASE_PATH="/mnt/10tb_hdd/testing_recursive_algo2/"
COLUMN_PCA1="PCA_1"
COLUMN_PCA2="PCA_2"
COLUMN_PCA3="PCA_3"
N_BINS=50
VERBOSE=0  # Default verbosity: 0 (off)

# Parse command-line arguments
while getopts "i:o:v:" opt; do
  case ${opt} in
    i ) INPUT_PATH=$OPTARG ;;
    o ) OUTPUT_BASE_PATH=$OPTARG ;;
    v ) VERBOSE=$OPTARG ;;
    \? ) echo "Usage: $0 [-i input_path] [-o output_base_path] [-v verbosity]"
         exit 1 ;;
  esac
done

# Start timer
start_time=$(date +%s)

# Clustering on PCA_1
echo "Clustering on $COLUMN_PCA1 dimension"
python clustering_dask_recursive.py --input_path "$INPUT_PATH" --col "$COLUMN_PCA1" --new_cluster_col "cluster_id_1" --n_bins "$N_BINS" --output_path "${OUTPUT_BASE_PATH}/id-1/" --verbosity "$VERBOSE"
echo "Clustering on $COLUMN_PCA1 dimension done"

# Clustering on PCA_2
echo "Starting clustering on $COLUMN_PCA2 dimension"
for i in {0..9}; do
  python clustering_dask_recursive.py --input_path "${OUTPUT_BASE_PATH}/id-1/cluster_id_1=$i/" --col "$COLUMN_PCA2" --new_cluster_col "cluster_id_2" --n_bins "$N_BINS" --output_path "${OUTPUT_BASE_PATH}/id-2/id-1_$i/" --verbosity "$VERBOSE"
  echo "$COLUMN_PCA2: $i / $N_BINS iterations completed"
done

# Clustering on PCA_3
echo "Starting clustering on $COLUMN_PCA3 dimension"
iteration=1
for i in {0..2}; do
  for j in {0..2}; do
    python clustering_dask_recursive.py --input_path "${OUTPUT_BASE_PATH}/id-2/id-1_$i/cluster_id_2=$j/" --col "$COLUMN_PCA3" --new_cluster_col "cluster_id_3" --n_bins "$N_BINS" --output_path "${OUTPUT_BASE_PATH}/id-3/id-1_$i/id-2_$j/" --verbosity "$VERBOSE"
    echo "$COLUMN_PCA3: $iteration / $((N_BINS**2)
    ((iteration++))  # Increment iteration counter
  done
done

# End timer and calculate execution time
end_time=$(date +%s)
elapsed_time=$((end_time - start_time))

echo "Total execution time: $elapsed_time seconds"