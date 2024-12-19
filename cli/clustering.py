# CLI for clustering using Dask and Spark. So far the fastest (and efficient) way of doing it. 
# CLI for fingerprint calculation on billions of molecules
import sys
import logging 
import os
import dask.dataframe as dd
from pydantic import ValidationError
from api.dask_clustering import Cluster
from api.utils.log_setup import setup_logging
from api.utils.common_arg_parser import common_arg_parser
from api.utils.config_loader import load_config
from api.utils.helper_functions import process_input, ProgressTracker
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, ntile, lit
from pyspark.sql.window import Window


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
    logging.info(f"Temporary folder: {config.TMP_FOLDER} ")
    # TODO: Add Spark RAM config 
    # TODO: Add tmp folder option, right now is on same path as OUTPUT_PATH
 
    # Initialize Cluster class.
    cluster = Cluster()

    ## Read the Dataframe containing the PCA coordinates
    pca_dataframe = dd.read_parquet(config.DATA_PATH, engine="pyarrow")

    logging.info("Data loaded") 
    logging.info("Clustering first dimension")

    # ============================================================
    # First-level clusters by PCA_1
    # ============================================================
    pca_dataframe, _ = cluster.assign_clusters_by_column(pca_dataframe, col='PCA_1', quantiles=config.STEPS_LIST[0], cluster_col="cluster_id")

    # Save first buckets in tmp directory. This will create parquet files that we can then load one by one with Pyspark to continue the clustering from bucke one
    # into the rest of the buckets
    # This turns out to be much faster than using dask. I suspect the reason is the graph that dask creates get's too large and complex. 

    logging.info("Saving first buckets to /tmp directory")
    tmp_directory = os.path.join(config.OUTPUT_PATH, "tmp")

    # Write each bucket from first dimension into a parquet (temporary) file
    pca_dataframe.to_parquet(tmp_directory, partition_on=["cluster_id_1"])

    # ============================================================
    # Second-level clusters by PCA_2 within each first-level cluster
    # ============================================================
    # For performance reasons the clustering on the following steps is done using spark API instead of Dask. 
    # TODO: Find a way of doing the whole pipeline in Dask. Probably persisting intermediate results 

    # Initialize SparkSession with optimized configurations
    spark = SparkSession.builder \
        .appName("GridClusteringJob") \
        .master("local[8]") \
        .config("spark.driver.memory", "50g") \
        .config("spark.executor.memory", "50g") \
        .config("spark.sql.shuffle.partitions", "2000") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.local.dir", config.TMP_FOLDER) \
        .getOrCreate()  

    # For every step in the first dim (i.e. for every bucket created in PCA_1 dimension), create new buckets
    for first_dim_step in range(config.STEPS_LIST[0]):
        print(f"Clustering bucket: {first_dim_step}")
        first_dim_path = os.path.join(tmp_directory, f"./cluster_id_1={first_dim_step}")
        bucket_dataframe = spark.read.parquet(first_dim_path)  

        # Repartition the DataFrame for better parallelism
        bucket_dataframe = bucket_dataframe.repartition(2000)

        #  Second of Clustering on PCA_2
        window_pca2 = Window.orderBy("PCA_2")
        bucket_dataframe = bucket_dataframe.withColumn("bin_PCA2", ntile(first_dim_step).over(window_pca2) - 1)

        # Repartition based on 'bin_PCA2' before next window function
        bucket_dataframe = bucket_dataframe.repartition("bin_PCA2")

        # Third Level of Clustering on PCA_3
        window_pca3 = Window.partitionBy("bin_PCA2").orderBy("PCA_3")
        bucket_dataframe = bucket_dataframe.withColumn("bin_PCA3", ntile(first_dim_step).over(window_pca3) - 1)

        # Convert bin_PCA2 and bin_PCA3 to integer type if necessary
        bucket_dataframe = bucket_dataframe.withColumn("bin_PCA2", col("bin_PCA2").cast("int"))
        bucket_dataframe = bucket_dataframe.withColumn("bin_PCA3", col("bin_PCA3").cast("int"))

        # Create cluster_id with the formula incorporating first_dim_step as bin_PCA1
        bucket_dataframe = bucket_dataframe.withColumn(
            "cluster_id",
            lit(first_dim_step) * (50 ** 2) + col("bin_PCA2") * 50 + col("bin_PCA3")
        )

        bucket_dataframe_result = bucket_dataframe.select("smiles", "PCA_1", "PCA_2", "PCA_3", "cluster_id")

        # Write each iteration's DataFrame to a separate folder
        # The final directory structure: /mnt/10tb_hdd/results/bucket_i/
        print(f"Writing dataframe")
        output_path = os.path.join(config.OUTPUT_PATH, f"bucket_{first_dim_step}")
        bucket_dataframe_result.coalesce(10).write.mode("overwrite").parquet(output_path)

        # TODO: Remove temporary directory? 
        del bucket_dataframe, bucket_dataframe_result