#!/usr/bin/env python3
"""
cluster_by_column.py

This is a standalone script meant to be used regressively (Divide and conquer). 
The idea is to run this script in parallel (via HPC job array for example) repeatedly 
for every subtable that we create. 

Essentially we want to split a large table (billions) into subtables using TDigest percentiles on a
selected column. Then for every subtable -which are smaller- do the same on a different column. 
That way we start splitting tables into smaller chunks and we 'cluster' the rows based on the similarity 
between values. We will get as many clusters as splits we did on every column 
For example, 50x50x50 will create 125,000 subtables from the original dataset where every subtable 
(out of the 125,000) will have similar values on the column_1 and column_2. 

For example, 


In essence, the script does the following:
    1) Reads one subtable (Parquet directory).
    For example the ones we got from splitting on PCA_2
    2) Clusters rows by a specified column into N bins via percentiles.
    3) Writes out partitioned Parquet files (one per bin). -> We create one parquet 
    per subtable to be able to run the script in parallel

Usage (example):
    python cluster_by_column.py \
        /path/to/subtable \
        --col PCA_3 \
        --cluster_col cluster_id_3 \
        --n_bins 50 \
        --output_path /path/to/output/third_level_split \
        --parquet_compression snappy

This script is designed to be run repeatedly in parallel (e.g., via an HPC job array),
each time for a different input subtable.
"""


import argparse
import os
import numpy as np
import dask.dataframe as dd
import dask.array as da

def parse_args():
    parser = argparse.ArgumentParser(
        description="Perform one hierarchical binning step and update a running cluster_id."
    )
    parser.add_argument(
        "--input_path",
        help="Path to the input Parquet directory (one subtable)."
    )
    parser.add_argument(
        "--col",
        help="Column name to cluster on (default: PCA_3)."
    )
    parser.add_argument(
        "--n_bins",
        type=int,
        default=50,
        help="Number of bins to use in this level of clustering (default: 50)."
    )
    parser.add_argument(
        "--output_path",
        help="Output directory for partitioned Parquet results (default: third_level_split)."
    )
    parser.add_argument(
        "--cluster_id_col",
        help="Name of the cumulative cluster-ID column (default: cluster_id)."
    )
    parser.add_argument(
        "--new_cluster_col",
        help="Name of the newly created cluster-ID column for this step (default: cluster_id_3)."
    )
    parser.add_argument(
        "--parquet_compression",
        default="snappy",
        help="Parquet compression codec (default: snappy)."
    )
    return parser.parse_args()


def compute_percentiles(df, col, quantiles):
    """
    Compute given percentiles for a column in a Dask DataFrame using T-Digest.
    
    Parameters
    ----------
    df : dask.dataframe.DataFrame
        The Dask DataFrame containing the data.
    col : str
        Column name for which the percentiles should be computed.
    quantiles : array-like
        Sequence of quantiles to compute (e.g. [0, 2, 4, ..., 98, 100]).
    
    Returns
    -------
    numpy.ndarray
        Array of computed percentile values corresponding to the given quantiles.
    """
    arr = df[col].to_dask_array(lengths=True)
    # Dask's T-Digest approach for large datasets
    return da.percentile(
        arr,
        q=quantiles,
        method='linear',
        internal_method='tdigest'
    ).compute()


def assign_cluster_ids(part, col, percentiles, new_col='cluster_id_new'):
    """
    Assign cluster IDs to a partition based on precomputed percentiles.
    
    Parameters
    ----------
    part : pandas.DataFrame
        A single partition of the Dask DataFrame.
    col : str
        Column name on which to assign new cluster IDs.
    percentiles : array-like
        An ordered array of percentile cut points.
    new_col : str, optional
        Name of the new column to store the cluster IDs (default: cluster_id_new).
    
    Returns
    -------
    pandas.DataFrame
        The partition with an additional column of new cluster IDs.
    """
    new_ids = np.digitize(part[col], percentiles) - 1
    # Ensure we don't exceed the last bin index
    max_bin_index = len(percentiles) - 2  # e.g., if 50 boundaries => 49 is max
    new_ids = np.clip(new_ids, 0, max_bin_index)
    part[new_col] = new_ids
    return part


def update_cumulative_cluster_id(part, new_col, cluster_col, n_bins):
    """
    Update the running cluster_id by combining it with a newly assigned cluster ID.

    cluster_id = (old_cluster_id * n_bins) + new_cluster_id

    If there is no old_cluster_id, assume it's zero (fresh start).
    """
    # If the old cluster_id column is missing, treat the old cluster_id as 0
    if cluster_col not in part.columns:
        part[cluster_col] = 0

    old_cluster_id = part[cluster_col].values
    new_cluster_id = part[new_col].values

    # Combine them into a single integer
    combined = old_cluster_id * n_bins + new_cluster_id
    part[cluster_col] = combined
    return part


def main():
    args = parse_args()

    # Prepare quantiles for n_bins. Example: n_bins=50 => step=2 => [0, 2, 4, ..., 98, 100]
    step_size = 100 / args.n_bins if args.n_bins else 100
    quantiles_range = np.arange(0, 100 + step_size, step_size)

    # 1) Read the subtable
    print(f"[INFO] Reading input from {args.input_path}")
    df = dd.read_parquet(args.input_path)

    # 2) Compute percentiles for the chosen column
    print(f"[INFO] Computing percentiles for column '{args.col}'...")
    percentiles = compute_percentiles(df, args.col, quantiles_range)
    print(f"[INFO] Percentile boundaries completed")

    # 3) Assign new cluster IDs for this level
    print(f"[INFO] Assigning new cluster IDs in column '{args.new_cluster_col}'...")
    df = df.map_partitions(
        assign_cluster_ids,
        col=args.col,
        percentiles=percentiles,
        new_col=args.new_cluster_col
    )

    # 4) Update the cumulative cluster_id
    #    cluster_id = (old_cluster_id * n_bins) + new_cluster_id
    print(f"[INFO] Updating cumulative cluster ID '{args.cluster_id_col}'...")
    # If this is the first split, we don't have cumulative cluster_id columns so we skip
    if args.cluster_id_col is not None:
        df = df.map_partitions(
            update_cumulative_cluster_id,
            new_col=args.new_cluster_col,
            cluster_col=args.cluster_id_col,
            n_bins=args.n_bins
        )
    # 5) Write out partitioned Parquet, partitioned by the final cluster_id
    #    (or you can partition by the newly created cluster_id_3 if you prefer.)
    os.makedirs(args.output_path, exist_ok=True)
    print(f"[INFO] Writing output to {args.output_path}, partitioned by '{args.new_cluster_col}'...")
    print(df.columns)
    df.to_parquet(
        args.output_path,
        partition_on=str(args.new_cluster_col),
       compression=args.parquet_compression
    )

    print("[INFO] Clustering + ID update complete!")



if __name__ == "__main__":
    import time
    s = time.time()
    main()
    e = time.time()
    print("Time took", (e-s))