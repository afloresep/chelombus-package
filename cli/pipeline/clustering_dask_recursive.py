#!/usr/bin/env python3
"""

This is a standalone script meant to be used regressively (Divide and conquer). 
The idea is to run this script in parallel (via HPC job array for example) repeatedly 
for every subtable that we create. To run in a single machine you can use `clustering.sh`
 

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
import logging 

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
    parser.add_argument(
    "--verbosity",
    type=int,
    default=0,
    help="Set verbosity level (0 for no logging, >0 for info logging)"
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



def main():
    import time 

    start = time.time()
    args = parse_args()

    # Configure logging: if verbosity > 0, show INFO messages; otherwise, only show WARNING and above.
    logging.basicConfig(level=logging.INFO if args.verbosity > 0 else logging.WARNING,
                        format='%(message)s')

    # Prepare quantiles for n_bins. Example: n_bins=50 => step=2 => [0, 2, 4, ..., 98, 100]
    step_size = 100 / args.n_bins if args.n_bins else 100
    quantiles_range = np.arange(0, 100 + step_size, step_size)

    # 1) Read the subtable
    logging.info(f"Reading input from {args.input_path}")
    df = dd.read_parquet(args.input_path)

    # 2) Compute percentiles for the chosen column
    logging.info(f"Computing percentiles for column '{args.col}'...")
    percentiles = compute_percentiles(df, args.col, quantiles_range)
    logging.info("Percentile boundaries completed")

    # 3) Assign new cluster IDs for this level
    logging.info(f"Assigning new cluster IDs in column '{args.new_cluster_col}'...")
    df = df.map_partitions(
        assign_cluster_ids,
        col=args.col,
        percentiles=percentiles,
        new_col=args.new_cluster_col
    )

    os.makedirs(args.output_path, exist_ok=True)


    """
    This script has to be generalizable to the clustering in all columns. For the first
    dimensions (PCA_1, PCA_2) we need to save the files using partition_on=[args.new_cluster_id]`.
    The reason is that dask will save the files based on the new_cluster_id, facilitating us to then use 
    a bash script to repeat the process for every partition (i.e. subtable). 

    For example, on the first run -clustering of PCA_1- args.new_cluster_id == cluster_id_1 so when we save using
    partition_on = 'cluster_id_1' and if we assume n_bins = 50 the directories will be saved like this:
    output_path/'cluster_pca_1_id=N_BIN'/
    So basically the name of the directory (metadata) is the each of the values that cluster_id_1 can take
    (50 different in this case). 
    Inside 'cluster_id_1=0' we will find the subtable of the original dataframe where cluster_id_1 = 0. 
    For clustering this again, -this time for PCA_2 column and new_cluster_id = cluster_id_2, 
    we can run a bash script with something like
    
    ```bash
    
    for i in {0..49}; do
        python clustering_dask_recursive.py --input_path /output_path/'cluster_pca_1_id=$i' --col PCA_2 --new_cluster_col cluster_id_2 --n_bins 50 --output_path output_path_2
        echo "Completed iteration $i"
     done
   ```
    """  
    if args.new_cluster_col == 'cluster_id_3':
        # This is the final step, thus we need to create the cluster_id
        # based on the results from previous cluster_id_1/2  
        df = df.astype({
            'cluster_id_1': 'int64',
            'cluster_id_2': 'int64',
            'cluster_id_3': 'int64'
        })
        # Ensure the computed cluster_id is also int64
        df['cluster_id'] = (df['cluster_id_1'] * (50 * 50) + df['cluster_id_2'] * 50 + df['cluster_id_3']).astype('int64') 

        # At this point, df has a single column 'cluster_id' representing the unique cluster index across all 3 levels.
        df = df.drop(columns=['cluster_id_1', 'cluster_id_2', 'cluster_id_3'])

        # Check correct columns
        logging.info(df.columns)
        # Now we don't need to partition based on column `parition_on=[args.new_cluster_col]`
        # So we can just partition the data as we need and save it
        df = df.repartition(npartitions=200)
        logging.info(f"[INFO] Writing output to {args.output_path}, partitioned by '{args.new_cluster_col}'...")
        df.to_parquet(args.output_path, engine='pyarrow', write_index=False)

        logging.info("[INFO] Clustering + ID update complete!")
        return 

    # Save as Parquet files
    logging.info(f"[INFO] Writing output to {args.output_path}, partitioned by '{args.new_cluster_col}'...")
    logging.info(df.columns)
    df.to_parquet(
        args.output_path,
        partition_on=str(args.new_cluster_col),
       compression=args.parquet_compression
    )

    end = time.time()
    logging.info("Time took " + str(end-start))

if __name__ == "__main__":
    main()
