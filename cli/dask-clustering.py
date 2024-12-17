# CLI for clustering using Dask. So far it does not work on a single machine due to complex graph being created. 
# TODO: Update and improve by persisting i.e. saving intermediate results 
# CLI for fingerprint calculation on billions of molecules
import dask.array as da
import sys
import pandas as pd
import logging 
import os
from tdigest import TDigest
import dask.dataframe as dd
import numpy as np
from pydantic import ValidationError
from api.dask_clustering import PCAClusterAssigner
from api.utils.log_setup import setup_logging
from api.utils.common_arg_parser import common_arg_parser
from api.utils.config_loader import load_config
from api.utils.helper_functions import process_input, ProgressTracker

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
    "STEPS_LIST":args.steps_list or config.STEPS_LIST,
    }) 

    os.makedirs(config.OUTPUT_PATH, exist_ok=True)

    logging.info(f"Input path: {config.DATA_PATH}")
    logging.info(f"Output directory: {config.OUTPUT_PATH}")
    logging.info(f"TDigest Model loaded: {config.TDIGEST_MODEL}")
    logging.info(f"Number of steps: {config.STEPS_LIST}")
    # TODO: Add Spark RAM config 
    # TODO: Add tmp folder option, right now is on same path as OUTPUT_PATH
 
    # Initialize Cluster class.
    cluster = PCAClusterAssigner()

    ## Read the Dataframe containing the PCA coordinates
    dataframe = dd.read_parquet(config.DATA_PATH, engine="pyarrow")

    logging.info("Data loaded") 
    logging.info("Clustering first dimension")

    # ============================================================
    # First-level clusters by PCA_1
    # ============================================================
    dataframe, _ = cluster.assign_clusters_by_column(dataframe, col='PCA_1', quantiles=config.STEPS_LIST[0], cluster_col="cluster_id")

    # ============================================================
    # Second-level clusters by PCA_2 within each first-level cluster
    # ============================================================o
    with ProgressTracker(description="Clustering", total_steps=config.STEPS_LIST[1], interval=60) as tracker:
        for i in range(config.STEPS_LIST[1]):
            pca2_percentiles = cluster.compute_percentiles(dataframe[dataframe['cluster_id'] == i], 'PCA_2', quantiles=config.STEPS_LIST[1])

            # Assign clusters conditionally, i.e. cluster_id_2 only to rows where cluster_id == i 
            df = cluster.assign_clusters_conditionally(
                df, 
                filter_col='cluster_id_1', 
                filter_val=i, 
                col='PCA_2', 
                cluster_col='cluster_id_2', 
                percentiles=pca2_percentiles)

            # Update progress bar
            tracker.update_progress()

        # Create a combined_id column. cluster_id_1 * 50² + cluster_id_2 * 50¹ + cluster_id_3 * 50⁰ 
        df = df.map_partitions(lambda part: part.assign(combined_id=part.cluster_id_1 * 50 + part.cluster_id_2))
    
    # Create a list with all the cluster_id (unique) values to iterate through them in the next step
    unique_cluster_id_labels= []

    for i in range(config.STEPS_LIST[0]):
        for j in range(config.STEPS_LIST[1]):
            unique_cluster_id_labels.append(i*50**2 + j*50)


    # Assert that the number of unique cluster ids match all the buckets created 
    assert len(unique_cluster_id_labels) == df.select('cluster_id').distinct().count()

    # ============================================================
    # Third-level clusters by PCA_3 within each second-level cluster
    # ============================================================
    # Assuming that our STEPS_LIST (i.e. number of buckets for each dimension) is 50x50x50, then: 
    # Up to this point we have created first 50 buckets on PCA_1 dimension and then 50 buckets based on PCA_2 dimension within each of those buckets. 
    # This gives us a total of 2500 buckets. For the clustering of the third and final dimension we have to go on each of those and subdivide them in 50 final buckets making a total 
    # of 50*3 buckets = 125,000 

    logging.info("Clustering for PCA_3")
    with ProgressTracker(description="Clustering",total_steps=(config.STEPS_LIST[2]*config.STEPS_LIST[1]), interval=60) as tracker:
        for i in range(unique_cluster_id_labels):

            # Third level clustering by PCA_3 within a specific bucket of PCA_2
            # Compute percentiles for PCA_2, but only for the subset where cluster_id == i
            pca3_percentiles = cluster.compute_percentiles(dataframe[dataframe['combined_id'] == i], 'PCA_3', quantiles=config.STEPS_LIST[2])

            # Assign clusters conditionally, i.e. cluster_id_2 only to rows where cluster_id == i 
            df = cluster.assign_clusters_conditionally(df, filter_col='combined_id', filter_val=i, col='PCA_3', cluster_col='cluster_id_3', percentiles=pca3_percentiles)

            # Update progress bar
            tracker.update_progress()


    # After you have assigned cluster_id_3, create a final combined_id_3 that includes all three cluster levels
    df = df.map_partitions(
        lambda part: part.assign(
            cluster_id=part.cluster_id_1 * 50 * 50 + part.cluster_id_2 * 50 + part.cluster_id_3
        )
    )


    # At this point, df has a single column 'combined_id' representing the unique cluster index across all 3 levels.
    df = df.drop(columns=['cluster_id_1', 'cluster_id_2', 'cluster_id_3'])

    logging.info("Cluster completed, saving results")
    df = df.repartition(npartitions=400)

    # Save as Parquet files
    output_path = "/mnt/10tb_hdd/buckets_pca_dask"
    df.to_parquet(output_path, engine='pyarrow', write_index=False)